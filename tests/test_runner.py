"""The worker loop: LISTEN task_queued, drain the queue, record results.

``process_next`` is run with fake jobs (no OpenTofu, no OpenStack); the slot
test runs a real ``Slot`` thread and checks that a committed task is picked
up because of the NOTIFY — there is no polling interval to fall back on.
"""

from __future__ import annotations

import json
import threading
import time
import uuid

import pytest
from sqlalchemy import insert, select, text

from app import job_context, job_queue, runner
from app.job_tables import TaskStatus, TaskType, task_events, tasks
from app.tasks import Failure
from app.utils.crypto import encrypt

PAYLOAD = {
    "app_id": "app-1",
    "app_git_link": "https://example.com/r.git",
    "release": "v1",
    "user_vars": {"terraform": {}},
    "teams": {},
    "openstack_envelope": {"auth_url": "x"},
    "state_token": "tok",
}


def _queue(factory, task_type=TaskType.DEPLOY, payload=None) -> uuid.UUID:
    task_id = uuid.uuid4()
    with factory() as s:
        s.execute(
            insert(tasks).values(
                taskId=task_id,
                deploymentId=uuid.uuid4(),
                type=task_type,
                status=TaskStatus.PENDING,
                payload=encrypt(json.dumps(payload or PAYLOAD)),
                created_at=job_queue.utcnow(),
            )
        )
        s.commit()
    return task_id


def _status(factory, task_id):
    with factory() as s:
        return s.execute(select(tasks.c.status).where(tasks.c.taskId == task_id)).scalar_one()


def _event_types(factory, task_id) -> list[str]:
    # Ordered by the stream cursor ``id``, which the admin (not the worker) may read.
    query = select(task_events.c.type).where(task_events.c.taskId == task_id).order_by(text("id"))
    with factory() as s:
        return list(s.scalars(query))


@pytest.mark.unit
def test_every_task_type_the_backend_queues_has_a_job():
    for task_type in (TaskType.DEPLOY, TaskType.DESTROY, TaskType.PAUSE, TaskType.RESUME, TaskType.REDEPLOY):
        assert callable(runner.JOBS[task_type])


@pytest.mark.unit
def test_run_job_maps_the_payload_and_builds_the_state_backend(monkeypatch):
    seen = {}

    def fake_job(events, **kwargs):
        seen.update(kwargs)
        return {}

    monkeypatch.setitem(runner.JOBS, TaskType.REDEPLOY, fake_job)
    claimed = job_queue.ClaimedTask(
        uuid.uuid4(), uuid.uuid4(), TaskType.REDEPLOY, {**PAYLOAD, "resource_address": "a.b"}
    )
    runner.run_job(claimed, events=None)

    assert seen["deployment_id"] == str(claimed.deployment_id)
    assert seen["resource_address"] == "a.b"
    backend = seen["state_backend"]
    assert backend.address.endswith(f"/internal/tfstate/{claimed.deployment_id}")
    assert (backend.username, backend.password) == (str(claimed.task_id), "tok")


@pytest.mark.integration
def test_process_next_records_success_after_all_events(queue_session_factory, queue_admin_factory, monkeypatch):
    f, admin = queue_session_factory, queue_admin_factory

    def fake_deploy(events, **kwargs):
        # Runs inside the slot's job context.
        assert job_context.current().deadline < float("inf")
        events.send_event("task-progress", phase="GIT_CLONE", progress_pct=10)
        events.send_event("task-log", message="working")
        return {"logs": [{"message": "done"}], "terraform_outputs": {"ip": {"value": "1"}}}

    monkeypatch.setitem(runner.JOBS, TaskType.DEPLOY, fake_deploy)
    task_id = _queue(admin)

    assert runner.process_next("w-0", 0, session_factory=f) is True
    assert runner.process_next("w-0", 0, session_factory=f) is False  # queue drained

    assert _status(admin, task_id) == TaskStatus.SUCCESS
    # The terminal event is the last one: the stream closes after it.
    assert _event_types(admin, task_id) == ["task-progress", "task-log", "task-succeeded"]


@pytest.mark.integration
def test_process_next_records_a_failure_without_retrying(queue_session_factory, queue_admin_factory, monkeypatch):
    f, admin = queue_session_factory, queue_admin_factory
    calls = []

    def failing(events, **kwargs):
        calls.append(1)
        raise Failure("OpenTofu apply failed", kwargs["deployment_id"], logs_dict=[{"message": "x"}])

    monkeypatch.setitem(runner.JOBS, TaskType.DESTROY, failing)
    task_id = _queue(admin, TaskType.DESTROY)

    runner.process_next("w-0", 0, session_factory=f)
    runner.process_next("w-0", 0, session_factory=f)

    assert calls == [1]  # not redelivered
    assert _status(admin, task_id) == TaskStatus.FAILED
    with admin() as s:
        logs, payload = s.execute(
            select(tasks.c.logs, task_events.c.payload)
            .join(task_events, task_events.c.taskId == tasks.c.taskId)
            .where(tasks.c.taskId == task_id)
        ).one()
    assert "[err] Error: OpenTofu apply failed" in logs
    assert json.loads(payload)["failure_kind"] == "worker_failure"


@pytest.mark.integration
@pytest.mark.timeout(30)
def test_slot_is_woken_by_task_queued_and_stops_on_shutdown(
    queue_session_factory, queue_admin_factory, queue_db_url, monkeypatch
):
    f, admin = queue_session_factory, queue_admin_factory
    done = threading.Event()

    def fake_pause(events, **kwargs):
        done.set()
        return {"logs": []}

    monkeypatch.setitem(runner.JOBS, TaskType.PAUSE, fake_pause)
    # The slot's process_next must use the test database.
    real_process_next = runner.process_next
    monkeypatch.setattr(runner, "process_next", lambda wid, slot: real_process_next(wid, slot, session_factory=f))

    stop = threading.Event()
    slot = runner.Slot("w-test", 0, stop, queue_db_url)
    thread = threading.Thread(target=slot.run)
    thread.start()
    try:
        time.sleep(0.5)  # the slot drained the empty queue and waits in recv()
        started = time.monotonic()
        task_id = _queue(admin, TaskType.PAUSE)
        assert done.wait(5), "slot was not woken by task_queued"
        assert time.monotonic() - started < 5
        deadline = time.monotonic() + 5
        while _status(admin, task_id) != TaskStatus.SUCCESS and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _status(admin, task_id) == TaskStatus.SUCCESS
    finally:
        stop.set()
        runner.wake_all_slots(queue_db_url)
        thread.join(timeout=10)
    assert not thread.is_alive()


@pytest.mark.unit
def test_worker_start_reports_undelivered_rescued_states(mocker, caplog):
    mocker.patch.object(runner, "pending_rescued_states", return_value=["/r/dep-1_task-1_t.tfstate.enc"])
    with caplog.at_level("WARNING", logger="app.runner"):
        assert runner.report_rescued_states() == ["/r/dep-1_task-1_t.tfstate.enc"]
    assert "make prod-restore-tfstate DEPLOYMENT=dep-1 FILE=/r/dep-1_task-1_t.tfstate.enc" in caplog.text
