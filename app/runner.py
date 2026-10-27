"""The worker process: claim tasks, run their jobs, record the results.

``python -m app`` starts ``WORKER_CONCURRENCY`` slots (threads). Each slot
keeps its own connection that ``LISTEN``s on ``task_queued``:

    LISTEN task_queued                  # before the first claim
    loop:
        claim and run until the queue is empty (SKIP LOCKED)
        block in the socket until the next notification

The backend's trigger sends ``task_queued`` when a PENDING task is
committed. One notification wakes every slot; ``SKIP LOCKED`` hands the
task to exactly one of them. There is no polling interval: an idle worker
sleeps in ``recv()`` and starts a new job as soon as it is committed.
Because the backlog is drained after every (re)connect, a notification
that arrived while nobody listened only delays a task, it never loses it.

On SIGTERM the slots stop claiming, running jobs finish, and the worker
sends itself a ``task_queued`` notification so the idle slots wake up and
exit. A worker killed mid-job leaves a task whose lease runs out; the
backend's reaper fails it — nothing is retried automatically.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
from typing import TYPE_CHECKING, Any

import psycopg

from . import job_context, job_queue, tasks
from .config import settings
from .db import SessionLocal, libpq_url
from .job_tables import CHANNEL_TASK_QUEUED, FAILURE_KIND_JOB, TaskStatus, TaskType
from .services.opentofu_executor import StateBackend, pending_rescued_states

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Task type -> job function. Every job takes (events, **payload kwargs).
JOBS: dict[TaskType, Callable[..., Any]] = {
    TaskType.DEPLOY: tasks.deploy_application,
    TaskType.DESTROY: tasks.destroy_deployment,
    TaskType.PAUSE: tasks.pause_deployment,
    TaskType.RESUME: tasks.resume_deployment,
    TaskType.REDEPLOY: tasks.redeploy_resource,
}

_RECONNECT_MIN_S = 1.0
_RECONNECT_MAX_S = 30.0
# Detect a silently dead LISTEN connection, so a slot reconnects instead of
# sleeping forever.
_KEEPALIVE = {"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 3}


def run_job(claimed: job_queue.ClaimedTask, events: job_queue.TaskEventSink) -> Any:
    """Call the job function for ``claimed`` with its decrypted payload.

    Maps the payload onto the job's keyword arguments and builds the
    OpenTofu HTTP state backend from the task's state token. Raises
    ValueError for a task type without a job; whatever the job raises
    (usually ``tasks.Failure``) propagates.
    """
    job = JOBS.get(TaskType(claimed.type))
    if job is None:
        raise ValueError(f"no job for task type {TaskType(claimed.type).value}")
    payload = claimed.payload
    kwargs: dict[str, Any] = {
        "deployment_id": str(claimed.deployment_id),
        "app_id": payload["app_id"],
        "app_git_link": payload["app_git_link"],
        "release": payload["release"],
        "user_vars": payload.get("user_vars") or {},
        "teams": payload.get("teams") or {},
        "openstack_envelope": payload.get("openstack_envelope"),
        "locale": payload.get("locale"),
    }
    if claimed.type == TaskType.REDEPLOY:
        kwargs["resource_address"] = payload.get("resource_address")
    token = payload.get("state_token")
    if token:
        kwargs["state_backend"] = StateBackend.for_job(str(claimed.deployment_id), str(claimed.task_id), token)
    return job(events, **kwargs)


def process_next(worker_id: str, slot: int = 0, session_factory=SessionLocal) -> bool:
    """Run one task if there is one. Returns whether there was one.

    Claims the task, runs its job under a renewed lease and inside the
    slot's ``job_context``, then records success or failure on the row.
    """
    with session_factory() as session:
        claimed = job_queue.claim_next(session, worker_id, settings.WORKER_LEASE_SECONDS)
    if claimed is None:
        return False
    logger.info(
        "running %s task %s for deployment %s",
        TaskType(claimed.type).value,
        claimed.task_id,
        claimed.deployment_id,
    )

    events = job_queue.TaskEventSink(session_factory, claimed.task_id)
    kind: str | None = None
    with job_queue.Lease(session_factory, claimed.task_id, worker_id, settings.WORKER_LEASE_SECONDS):
        try:
            with job_context.bind(job_context.for_slot(slot)):
                result = run_job(claimed, events)
        except Exception as e:
            logger.info("task %s failed: %s", claimed.task_id, e)
            status, columns, kind = TaskStatus.FAILED, job_queue.failure_columns(e), FAILURE_KIND_JOB
        else:
            status, columns = TaskStatus.SUCCESS, job_queue.success_columns(result)
        finally:
            # All progress/log events are in the table before the terminal one.
            events.close()
    job_queue.record_result(session_factory, claimed, worker_id, status, columns, kind)
    logger.info("task %s finished: %s", claimed.task_id, status.value)
    return True


class Slot:
    """One worker thread: LISTEN, drain the queue, wait in the socket, repeat."""

    def __init__(self, worker_id: str, slot: int, stop: threading.Event, conninfo: str) -> None:
        self.worker_id = worker_id
        self.slot = slot
        self.stop = stop
        self.conninfo = conninfo

    def run(self) -> None:
        delay = _RECONNECT_MIN_S
        while not self.stop.is_set():
            try:
                with psycopg.connect(self.conninfo, autocommit=True, **_KEEPALIVE) as conn:
                    # LISTEN before the first claim; otherwise a task queued
                    # between the claim and the LISTEN would wait for the next one.
                    conn.execute(f"LISTEN {CHANNEL_TASK_QUEUED}")
                    delay = _RECONNECT_MIN_S
                    while not self.stop.is_set():
                        while not self.stop.is_set() and process_next(self.worker_id, self.slot):
                            pass
                        if self.stop.is_set():
                            break
                        # Blocks in the socket until the next notification.
                        for _ in conn.notifies(stop_after=1):
                            pass
            except Exception:
                if self.stop.is_set():
                    break
                # Most likely the database went away. Back off and reconnect;
                # the backlog is drained again right after.
                logger.exception("worker slot %s: database error, reconnecting in %.0fs", self.slot, delay)
                self.stop.wait(delay)
                delay = min(delay * 2, _RECONNECT_MAX_S)


def wake_all_slots(conninfo: str) -> None:
    """Send ``task_queued`` so slots blocked in ``notifies()`` wake up (used on shutdown)."""
    try:
        with psycopg.connect(conninfo, autocommit=True) as conn:
            conn.execute("SELECT pg_notify(%s, '')", (CHANNEL_TASK_QUEUED,))
    except Exception:
        logger.warning("could not wake the worker slots for shutdown", exc_info=True)


def report_rescued_states() -> list[str]:
    """Log every rescued OpenTofu state still waiting to be restored.

    Such a file exists when the API was unreachable for longer than the
    worker kept retrying, or the worker died while retrying. The task log
    names it too, unless the worker died first — hence this reminder.
    """
    pending = pending_rescued_states()
    for path in pending:
        deployment_id = os.path.basename(path).split("_", 1)[0]
        logger.warning(
            "rescued OpenTofu state not restored yet: %s — restore it with "
            "`make prod-restore-tfstate DEPLOYMENT=%s FILE=%s` (backend scripts/restore_tfstate.py)",
            path,
            deployment_id,
            path,
        )
    return pending


def check_health(threads: list[threading.Thread], stop: threading.Event, conninfo: str) -> bool:
    """Whether the worker is fit: every slot still runs and the database answers.

    During shutdown idle slots exit on purpose, so only the database counts.
    """
    if not stop.is_set() and not all(t.is_alive() for t in threads):
        logger.error("health check: a worker slot has died")
        return False
    try:
        with psycopg.connect(conninfo, connect_timeout=5) as conn:
            conn.execute("SELECT 1")
    except Exception as e:
        logger.warning("health check: database not reachable (%s)", e)
        return False
    return True


def mark_healthy(path: str) -> None:
    """Touch ``path``; the image's HEALTHCHECK fails once it is older than three intervals.

    Never deleted when unhealthy, only left to go stale: a job's tools run
    as another user and cannot touch a file the worker (root) owns.
    """
    with open(path, "a"):
        pass
    os.utime(path)


def report_health(threads: list[threading.Thread], stop: threading.Event, done: threading.Event, conninfo: str) -> None:
    """Check health every ``WORKER_HEALTH_INTERVAL_SECONDS`` until ``done``.

    Runs beside the slots, so a long apply in every slot does not make the
    worker look dead; during a graceful shutdown it keeps reporting until
    the running jobs have finished.
    """
    while True:
        if check_health(threads, stop, conninfo):
            try:
                mark_healthy(settings.WORKER_HEALTH_FILE)
            except OSError:
                logger.warning("health check: cannot write %s", settings.WORKER_HEALTH_FILE, exc_info=True)
        if done.wait(settings.WORKER_HEALTH_INTERVAL_SECONDS):
            return


def main() -> None:
    """Start the slots, install the SIGTERM/SIGINT handlers and wait for shutdown."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    conninfo = libpq_url(settings.DATABASE_URL)
    stop = threading.Event()

    def _shutdown(signum: int, _frame: Any) -> None:
        logger.info("signal %s: finishing running jobs, claiming no new ones", signum)
        stop.set()
        # Off the signal handler: the slots blocked in recv() need a NOTIFY.
        threading.Thread(target=wake_all_slots, args=(conninfo,), daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    if settings.WORKER_JOB_UID_BASE is not None:
        # Job directories and slot homes: traversable, not listable, for the
        # slot users (a volume mounted over them comes up 0777, so set it).
        for base in (settings.TEMP_REPO_BASE_PATH, settings.WORKER_JOB_HOME_BASE):
            os.makedirs(base, exist_ok=True)
            os.chmod(base, 0o711)

    report_rescued_states()

    threads = [
        threading.Thread(
            target=Slot(f"{worker_id}-{i}", i, stop, conninfo).run,
            name=f"worker-slot-{i}",
        )
        for i in range(settings.WORKER_CONCURRENCY)
    ]
    logger.info("worker %s listening on %s with %d slots", worker_id, CHANNEL_TASK_QUEUED, len(threads))
    for t in threads:
        t.start()
    done = threading.Event()
    threading.Thread(
        target=report_health, args=(threads, stop, done, conninfo), name="worker-health", daemon=True
    ).start()
    for t in threads:
        t.join()
    done.set()
    logger.info("worker %s stopped", worker_id)
