"""Claim tasks from the queue, report on them while they run, record the result.

The queue is the backend's ``tasks`` table. A task is claimed with
``SELECT … FOR UPDATE SKIP LOCKED``, so any number of worker slots can look
at the same table without handing one task to two of them. While the job
runs, a lease on the row is renewed; if the worker dies the lease runs out
and the backend's reaper fails the task. Nothing is retried automatically:
a half-applied OpenTofu run is not something to repeat blindly (before,
Celery's ``task_acks_late`` redelivered it after a crash and ``apply`` ran
twice).

The result goes straight onto the row — logs, outputs, status and the
terminal event in one transaction. No result backend, no event bus, and no
regex over a traceback to get the error back out.
"""

from __future__ import annotations

import json
import logging
import threading
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update

from .job_tables import (
    EVENT_FAILED,
    EVENT_PROGRESS,
    EVENT_SUCCEEDED,
    FAILURE_KIND_JOB,
    TaskStatus,
    TaskType,
    task_events,
    tasks,
)
from .utils.crypto import InvalidToken, decrypt

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Naive UTC, like every DateTime column of the backend schema."""
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class ClaimedTask:
    """A task this worker now runs, with its decrypted job payload."""

    task_id: uuid.UUID
    deployment_id: uuid.UUID
    type: TaskType
    payload: dict[str, Any]


# ----------------------------------------------------------------
# Claiming
# ----------------------------------------------------------------
def claim_next(session: Session, worker_id: str, lease_seconds: int) -> ClaimedTask | None:
    """Take the oldest pending task, or return None when there is none.

    The payload is decrypted and removed from the row in the same
    transaction: from here on it exists only in this worker's memory.
    """
    row = session.execute(
        select(tasks.c.taskId, tasks.c.deploymentId, tasks.c.type, tasks.c.payload)
        .where(tasks.c.status == TaskStatus.PENDING)
        .order_by(tasks.c.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    ).first()
    if row is None:
        session.rollback()
        return None
    task_id, deployment_id, task_type, sealed = row

    try:
        if sealed is None:
            raise ValueError("the task has no job payload")
        payload = json.loads(decrypt(bytes(sealed)))
    except (InvalidToken, ValueError) as e:
        # Unrunnable as queued (e.g. backend and worker disagree on the key).
        # Failing it here beats leaving it pending for the next slot to
        # trip over.
        logger.error("cannot run task %s: %s", task_id, e)
        _finish(
            session,
            task_id,
            deployment_id,
            task_type,
            TaskStatus.FAILED,
            {"logs": f"Task failed: {e}", "payload": None},
            FAILURE_KIND_JOB,
        )
        session.commit()
        return None

    now = utcnow()
    session.execute(
        update(tasks)
        .where(tasks.c.taskId == task_id)
        .values(
            status=TaskStatus.RUNNING,
            payload=None,
            claimed_by=worker_id,
            started_at=now,
            lease_until=now + timedelta(seconds=lease_seconds),
        )
    )
    session.commit()
    return ClaimedTask(task_id, deployment_id, task_type, payload)


# ----------------------------------------------------------------
# While a job runs
# ----------------------------------------------------------------
class TaskEventSink:
    """Writes a running job's events to ``task_events``, a few at a time.

    The job calls ``send_event`` (from its own thread and from the tool
    output reader threads). A background thread wakes up when events are
    waiting, lets ``linger_s`` pass to collect the lines that follow, and
    writes them in one transaction: one commit and — Postgres folds
    identical notifications of a transaction — one ``task_events`` NOTIFY
    per batch instead of one per line. Progress events also update the
    task's ``current_phase``/``progress_pct``, so a page reload shows where
    the task is.

    A failed write is logged and dropped: losing a log line must not fail
    an OpenTofu run that is otherwise fine (the full log also goes onto the
    task row at the end). ``close`` flushes what is left.
    """

    def __init__(self, session_factory, task_id: uuid.UUID, *, linger_s: float = 0.2) -> None:
        self._session_factory = session_factory
        self._task_id = task_id
        self._linger_s = linger_s
        self._pending: list[tuple[str, dict[str, Any]]] = []
        self._cond = threading.Condition()
        self._closing = False
        self._thread = threading.Thread(target=self._run, name=f"events-{task_id}", daemon=True)
        self._thread.start()

    def send_event(self, event_type: str, **payload: Any) -> None:
        """Queue one event; the browser receives ``{"type": event_type, **payload}``."""
        with self._cond:
            self._pending.append((event_type, {"type": event_type, **payload}))
            self._cond.notify()

    def close(self) -> None:
        """Write everything still pending and stop the writer thread."""
        with self._cond:
            self._closing = True
            self._cond.notify()
        self._thread.join()

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._pending and not self._closing:
                    self._cond.wait()
                if not self._pending and self._closing:
                    return
                closing = self._closing
            if not closing:
                # Collect the lines that follow (tool output comes in bursts).
                with self._cond:
                    self._cond.wait_for(lambda: self._closing, timeout=self._linger_s)
            with self._cond:
                batch, self._pending = self._pending, []
            self._write(batch)

    def _write(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        if not batch:
            return
        now = utcnow()
        rows = [
            {"taskId": self._task_id, "type": t, "payload": json.dumps(body, default=str), "created_at": now}
            for t, body in batch
        ]
        progress = [body for t, body in batch if t == EVENT_PROGRESS]
        session = self._session_factory()
        try:
            session.execute(insert(task_events), rows)
            if progress:
                last = progress[-1]
                changes: dict[str, Any] = {}
                if last.get("phase") is not None:
                    changes["current_phase"] = str(last["phase"])[:50]
                if last.get("progress_pct") is not None:
                    changes["progress_pct"] = max(0, min(100, int(last["progress_pct"])))
                if changes:
                    session.execute(update(tasks).where(tasks.c.taskId == self._task_id).values(**changes))
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("could not record %d event(s) for task %s", len(rows), self._task_id, exc_info=True)
        finally:
            session.close()


class Lease:
    """Renews this worker's claim on a task while its job runs.

    Renewing only succeeds while the row still says it is ours and running.
    If the reaper got there first (the renewals stalled for a whole lease),
    ``lost`` is set; the job is not interrupted — its result is still worth
    recording — but the task's status is no longer this worker's to set.
    """

    def __init__(self, session_factory, task_id: uuid.UUID, worker_id: str, lease_seconds: int) -> None:
        self._session_factory = session_factory
        self._task_id = task_id
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"lease-{task_id}", daemon=True)
        self.lost = False

    def __enter__(self) -> Lease:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()

    def renew(self) -> bool:
        """Extend the lease by ``lease_seconds``; False if the row is no longer ours and running."""
        session = self._session_factory()
        try:
            result = session.execute(
                update(tasks)
                .where(
                    tasks.c.taskId == self._task_id,
                    tasks.c.claimed_by == self._worker_id,
                    tasks.c.status == TaskStatus.RUNNING,
                )
                .values(lease_until=utcnow() + timedelta(seconds=self._lease_seconds))
            )
            session.commit()
            return bool(getattr(result, "rowcount", 0))
        finally:
            session.close()

    def _run(self) -> None:
        """Renew every third of the lease until stopped or the lease is lost."""
        while not self._stop.wait(self._lease_seconds / 3):
            try:
                if not self.renew():
                    self.lost = True
                    logger.error("lost the lease on task %s", self._task_id)
                    return
            except Exception:
                # A database blip; the next tick tries again, and the lease
                # has two more intervals before it runs out.
                logger.warning("could not renew the lease on task %s", self._task_id, exc_info=True)


# ----------------------------------------------------------------
# Recording the result
# ----------------------------------------------------------------
def _json_text(value: Any, *, indent: int | None = None) -> str | None:
    """A value for a text column: strings as-is, anything else as JSON, empty as None."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=indent, ensure_ascii=False, default=str)


def success_columns(result: Any) -> dict[str, Any]:
    """Task columns for a job function's return value."""
    result = result if isinstance(result, dict) else {}
    outputs = result.get("terraform_outputs")
    return {
        "logs": _json_text(result.get("logs")),
        # An empty dict (destroy, pause, resume) leaves the column empty, so
        # the deploy's outputs stay the latest ones on record.
        "outputs": _json_text(outputs) if outputs else None,
    }


def success_event_extra(result: Any) -> dict[str, Any]:
    """Extra fields for the terminal ``task-succeeded`` *event only*.

    Lab jobs (``lab_add_student``/``lab_reset_student`` with a new
    password/``lab_reassign_student`` as ``passwd``) return a one-time
    generated/rotated password under ``result["lab_result"]``. It must
    reach the triggering instructor exactly once and never be persisted:
    not in ``tasks.outputs`` (read repeatedly by ``get_deployment_outputs``
    for unrelated purposes — see ``success_columns``, which deliberately
    does not look at this key), only in this one ``task_events`` row.
    """
    result = result if isinstance(result, dict) else {}
    lab_result = result.get("lab_result")
    return {"lab_result": lab_result} if isinstance(lab_result, dict) else {}


def failure_columns(error: BaseException) -> dict[str, Any]:
    """Task columns for a job that raised.

    A ``Failure`` carries what the job produced before it failed — the log
    and the outputs of a half-finished apply — and all of it is kept. The
    log ends with the error line and the commit, as it always did.
    Anything else is a bug in the worker; the traceback goes into the log.
    """
    from .tasks import Failure  # the jobs module imports this one

    if not isinstance(error, Failure):
        details = "".join(traceback.format_exception(error)).strip()
        return {"logs": f"Task failed: {details}"}

    logs = _json_text(error.logs_dict) or ""
    logs += f"\n\n[err] Error: {error.message}"
    commit = error.commit_info
    if isinstance(commit, dict) and commit:
        logs += f"\nCommit: {str(commit.get('hash', 'N/A'))[:8]}"
        logs += f"\n   Message: {commit.get('message', 'N/A')}"
        logs += f"\n   Author: {commit.get('author', 'N/A')}"
    columns: dict[str, Any] = {"logs": logs}
    if isinstance(error.terraform_outputs, dict):
        columns["outputs"] = json.dumps(error.terraform_outputs, indent=2)
    return columns


def _terminal_event(
    task_id: uuid.UUID,
    deployment_id: uuid.UUID,
    task_type: TaskType,
    status: TaskStatus,
    failure_kind: str | None,
    event_extra: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    """The last event of a task's stream (same shape the backend's reaper writes).

    ``event_extra`` (see ``success_event_extra``) rides in this event only —
    it is never written to any ``tasks`` column.
    """
    event_type = EVENT_SUCCEEDED if status == TaskStatus.SUCCESS else EVENT_FAILED
    payload: dict[str, Any] = {
        "type": event_type,
        "deployment_id": str(deployment_id),
        "task_id": str(task_id),
        "task_type": TaskType(task_type).value,
        "status": status.value,
    }
    if status == TaskStatus.FAILED and failure_kind:
        payload["failure_kind"] = failure_kind
    if event_extra:
        payload.update(event_extra)
    return event_type, payload


def _finish(
    session: Session,
    task_id: uuid.UUID,
    deployment_id: uuid.UUID,
    task_type: TaskType,
    status: TaskStatus,
    columns: dict[str, Any],
    failure_kind: str | None = None,
    event_extra: dict[str, Any] | None = None,
) -> None:
    """Set the terminal status and result columns and append the terminal event. Does not commit."""
    session.execute(
        update(tasks)
        .where(tasks.c.taskId == task_id)
        .values(**columns, status=status, finished_at=utcnow(), lease_until=None)
    )
    event_type, payload = _terminal_event(task_id, deployment_id, task_type, status, failure_kind, event_extra)
    session.execute(
        insert(task_events).values(taskId=task_id, type=event_type, payload=json.dumps(payload), created_at=utcnow())
    )


def record_result(
    session_factory,
    claimed: ClaimedTask,
    worker_id: str,
    status: TaskStatus,
    columns: dict[str, Any],
    failure_kind: str | None = None,
    event_extra: dict[str, Any] | None = None,
) -> None:
    """Store a finished job's result and end the task's event stream.

    Results are always stored — the outputs describe what exists in
    OpenStack whatever the bookkeeping says. The status is only set while
    the task is still ours; if the reaper already failed it, it stays
    failed. The status change fires ``task_finished`` in the backend.
    """
    session = session_factory()
    try:
        row = session.execute(
            select(tasks.c.status, tasks.c.claimed_by).where(tasks.c.taskId == claimed.task_id).with_for_update()
        ).one()
        if row.status == TaskStatus.RUNNING and row.claimed_by == worker_id:
            _finish(
                session, claimed.task_id, claimed.deployment_id, claimed.type, status, columns, failure_kind, event_extra
            )
        else:
            logger.warning(
                "task %s finished after its lease was taken over; keeping its status %s",
                claimed.task_id,
                row.status,
            )
            kept = {name: value for name, value in columns.items() if value is not None}
            if kept:
                session.execute(update(tasks).where(tasks.c.taskId == claimed.task_id).values(**kept))
        session.commit()
    finally:
        session.close()
