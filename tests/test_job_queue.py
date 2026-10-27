"""The worker's side of the Postgres job queue (``app/job_queue.py``).

Runs against a throw-away database with the queue tables (see conftest).
The code under test connects as a role with exactly the backend's worker
grants (``queue_session_factory``); setup and checks use the owner
(``queue_admin_factory``).
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import timedelta

import psycopg
import pytest
from sqlalchemy import insert, select, text, update

from app import job_queue
from app.job_tables import TaskStatus, TaskType, task_events, tasks
from app.tasks import Failure
from app.utils.crypto import encrypt

pytestmark = pytest.mark.integration


def _queue(factory, *, payload: dict | None = None, sealed: bytes | None = None, age_s: int = 0) -> uuid.UUID:
    """Insert a PENDING task as the backend does; returns its id."""
    task_id = uuid.uuid4()
    with factory() as s:
        s.execute(
            insert(tasks).values(
                taskId=task_id,
                deploymentId=uuid.uuid4(),
                type=TaskType.DEPLOY,
                status=TaskStatus.PENDING,
                payload=sealed if sealed is not None else encrypt(json.dumps(payload or {"app_id": "a"})),
                created_at=job_queue.utcnow() - timedelta(seconds=age_s),
            )
        )
        s.commit()
    return task_id


def _row(factory, task_id):
    with factory() as s:
        return s.execute(select(tasks).where(tasks.c.taskId == task_id)).one()


def _events(factory, task_id) -> list[tuple[str, dict]]:
    with factory() as s:
        rows = s.execute(
            select(task_events.c.type, task_events.c.payload)
            .where(task_events.c.taskId == task_id)
            .order_by(text("id"))  # the admin may read the stream cursor
        ).all()
    return [(t, json.loads(p)) for t, p in rows]


# ----------------------------------------------------------------
# claiming
# ----------------------------------------------------------------
def test_claim_takes_the_oldest_and_removes_the_payload(queue_session_factory, queue_admin_factory):
    f, admin = queue_session_factory, queue_admin_factory
    newer = _queue(admin, payload={"app_id": "new"})
    older = _queue(admin, payload={"app_id": "old"}, age_s=60)

    with f() as s:
        claimed = job_queue.claim_next(s, "w-0", lease_seconds=120)

    assert claimed is not None and claimed.task_id == older
    assert claimed.payload == {"app_id": "old"}
    row = _row(admin, older)
    assert row.status == TaskStatus.RUNNING
    assert row.claimed_by == "w-0" and row.started_at is not None
    assert row.lease_until > job_queue.utcnow() + timedelta(seconds=100)
    # The credentials now exist only in the worker's memory.
    assert row.payload is None
    assert _row(admin, newer).status == TaskStatus.PENDING


def test_empty_queue_returns_none(queue_session_factory):
    with queue_session_factory() as s:
        assert job_queue.claim_next(s, "w-0", 120) is None


def test_concurrent_slots_never_get_the_same_task(queue_session_factory, queue_admin_factory):
    f = queue_session_factory
    task_id = _queue(queue_admin_factory)
    start = threading.Barrier(4)
    got: list = []

    def slot(i: int) -> None:
        with f() as s:
            start.wait()
            got.append(job_queue.claim_next(s, f"w-{i}", 120))

    threads = [threading.Thread(target=slot, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    claimed = [c for c in got if c is not None]
    assert len(got) == 4 and len(claimed) == 1
    assert claimed[0].task_id == task_id


def test_undecryptable_payload_fails_the_task_with_a_terminal_event(queue_session_factory, queue_admin_factory):
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin, sealed=b"not-a-fernet-token")

    with f() as s:
        assert job_queue.claim_next(s, "w-0", 120) is None

    row = _row(admin, task_id)
    assert row.status == TaskStatus.FAILED and row.payload is None
    assert row.logs.startswith("Task failed:")
    [(event_type, payload)] = _events(admin, task_id)
    assert event_type == "task-failed" and payload["failure_kind"] == "worker_failure"


# ----------------------------------------------------------------
# events
# ----------------------------------------------------------------
def test_event_sink_writes_all_events_in_order_and_batched(queue_session_factory, queue_admin_factory, queue_db_url):
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin)
    with f() as s:
        job_queue.claim_next(s, "w-0", 120)

    with psycopg.connect(queue_db_url, autocommit=True) as listener:
        listener.execute("LISTEN task_events")
        sink = job_queue.TaskEventSink(f, task_id, linger_s=0.2)
        for i in range(100):
            sink.send_event("task-log", message=f"line {i}")
        sink.send_event("task-progress", phase="TERRAFORM_APPLY", progress_pct=80)
        sink.close()
        notifications = list(listener.notifies(timeout=0.5))

    events = _events(admin, task_id)
    assert [p["message"] for t, p in events if t == "task-log"] == [f"line {i}" for i in range(100)]
    assert events[-1] == ("task-progress", {"type": "task-progress", "phase": "TERRAFORM_APPLY", "progress_pct": 80})
    # 101 events, but a handful of transactions: one NOTIFY per batch.
    assert 1 <= len(notifications) <= 5
    row = _row(admin, task_id)
    assert row.current_phase == "TERRAFORM_APPLY" and row.progress_pct == 80


def test_event_sink_delivers_a_lone_line_without_waiting_for_more(queue_session_factory, queue_admin_factory):
    """A single line must show up promptly (linger), not when the job ends."""
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin)
    sink = job_queue.TaskEventSink(f, task_id, linger_s=0.1)
    try:
        sink.send_event("task-log", message="hello")
        deadline = time.monotonic() + 3
        while not _events(admin, task_id) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _events(admin, task_id) == [("task-log", {"type": "task-log", "message": "hello"})]
    finally:
        sink.close()


# ----------------------------------------------------------------
# lease
# ----------------------------------------------------------------
def test_lease_renews_while_ours_and_reports_loss(queue_session_factory, queue_admin_factory):
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin)
    with f() as s:
        job_queue.claim_next(s, "w-0", 120)
    lease = job_queue.Lease(f, task_id, "w-0", lease_seconds=600)

    assert lease.renew() is True
    assert _row(admin, task_id).lease_until > job_queue.utcnow() + timedelta(seconds=500)

    with admin() as s:  # the backend's reaper took it
        s.execute(update(tasks).where(tasks.c.taskId == task_id).values(status=TaskStatus.FAILED))
        s.commit()
    assert lease.renew() is False


# ----------------------------------------------------------------
# result
# ----------------------------------------------------------------
def test_success_is_recorded_with_a_terminal_event(queue_session_factory, queue_admin_factory):
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin)
    with f() as s:
        claimed = job_queue.claim_next(s, "w-0", 120)

    columns = job_queue.success_columns({"logs": [{"message": "ok"}], "terraform_outputs": {"ip": {"value": "1"}}})
    job_queue.record_result(f, claimed, "w-0", TaskStatus.SUCCESS, columns)

    row = _row(admin, task_id)
    assert row.status == TaskStatus.SUCCESS and row.finished_at and row.lease_until is None
    assert json.loads(row.logs) == [{"message": "ok"}]
    assert json.loads(row.outputs) == {"ip": {"value": "1"}}
    [(event_type, payload)] = _events(admin, task_id)
    assert event_type == "task-succeeded"
    assert payload == {
        "type": "task-succeeded",
        "deployment_id": str(claimed.deployment_id),
        "task_id": str(task_id),
        "task_type": "deploy",
        "status": "success",
    }


def test_result_after_the_reaper_keeps_its_status_but_stores_the_logs(queue_session_factory, queue_admin_factory):
    f, admin = queue_session_factory, queue_admin_factory
    task_id = _queue(admin)
    with f() as s:
        claimed = job_queue.claim_next(s, "w-0", 120)
    with admin() as s:  # the backend's reaper took it
        s.execute(update(tasks).where(tasks.c.taskId == task_id).values(status=TaskStatus.FAILED))
        s.commit()

    job_queue.record_result(f, claimed, "w-0", TaskStatus.SUCCESS, {"logs": "late", "outputs": None})

    row = _row(admin, task_id)
    assert row.status == TaskStatus.FAILED and row.logs == "late"
    assert _events(admin, task_id) == []  # the reaper wrote the terminal event


# ----------------------------------------------------------------
# result columns (no database)
# ----------------------------------------------------------------
@pytest.mark.unit
def test_failure_columns_keep_logs_error_commit_and_outputs():
    err = Failure(
        "OpenTofu apply failed",
        "dep",
        logs_dict=[{"message": "x"}],
        commit_info={"hash": "abcdef123456", "message": "m", "author": "a"},
        terraform_outputs={"ip": 1},
    )
    cols = job_queue.failure_columns(err)
    assert cols["logs"].startswith('[{"message": "x"}]')
    assert "[err] Error: OpenTofu apply failed" in cols["logs"]
    assert "Commit: abcdef12" in cols["logs"]
    assert json.loads(cols["outputs"]) == {"ip": 1}


@pytest.mark.unit
def test_failure_columns_for_a_worker_bug_contain_the_traceback():
    try:
        raise KeyError("app_id")
    except KeyError as e:
        cols = job_queue.failure_columns(e)
    assert cols["logs"].startswith("Task failed:") and "KeyError" in cols["logs"]
    assert "outputs" not in cols


@pytest.mark.unit
def test_success_columns_leave_outputs_empty_for_lifecycle_jobs():
    assert job_queue.success_columns({"logs": [], "terraform_outputs": {}})["outputs"] is None
