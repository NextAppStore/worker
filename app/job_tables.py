"""The worker's view of the backend database: the job queue, nothing else.

Backend and worker are separate repositories without shared code. The
worker connects as ``appstore_worker``; the backend migration
``f8a9b0c1d2e3_worker_role`` grants that role exactly the columns mapped
below, and the backend's ``tests/test_worker_contract.py`` pins them. A
column missing here is one the worker may not touch; a column added here
without a grant fails at runtime with "permission denied". So: change both
together, worker first.

Also the job contract (event names, payload keys) mirrors the backend's
``app/services/jobs.py``.
"""

from __future__ import annotations

import enum

from sqlalchemy import Column, DateTime, Enum, Integer, LargeBinary, MetaData, String, Table, Text
from sqlalchemy.dialects.postgresql import UUID


class TaskStatus(enum.StrEnum):
    # Stored by name (uppercase) in the Postgres enum ``taskstatus``.
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(enum.StrEnum):
    # Stored by name (uppercase) in the Postgres enum ``tasktype``.
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"
    PAUSE = "pause"
    RESUME = "resume"
    REDEPLOY = "redeploy"
    # Kubernetes-App student-pod management (labctl ported, backend#37).
    # ``passwd`` is not its own type — it rotates the secret then reassigns,
    # carried by LAB_REASSIGN with an extra payload flag.
    LAB_REASSIGN = "lab_reassign"
    LAB_RESET = "lab_reset"
    LAB_ADD_STUDENT = "lab_add_student"
    LAB_REMOVE_STUDENT = "lab_remove_student"


metadata = MetaData()

# SELECT: taskId, deploymentId, type, status, payload, claimed_by, created_at
# UPDATE: status, payload, claimed_by, lease_until, started_at, finished_at,
#         logs, outputs, current_phase, progress_pct
tasks = Table(
    "tasks",
    metadata,
    Column("taskId", UUID(as_uuid=True), primary_key=True),
    Column("deploymentId", UUID(as_uuid=True), nullable=False),
    Column("type", Enum(TaskType, name="tasktype", create_type=False), nullable=False),
    Column("status", Enum(TaskStatus, name="taskstatus", create_type=False)),
    Column("payload", LargeBinary),
    Column("claimed_by", String),
    Column("lease_until", DateTime),
    Column("started_at", DateTime),
    Column("finished_at", DateTime),
    Column("logs", Text),
    Column("outputs", Text),
    Column("current_phase", String(50)),
    Column("progress_pct", Integer),
    Column("created_at", DateTime),
)

# INSERT: taskId, type, payload, created_at. The ``id`` column (a bigserial,
# the live stream's cursor) is deliberately not mapped: the worker never
# reads it, and with it mapped SQLAlchemy would either add ``RETURNING id``
# or fetch the next id itself and insert it — both need privileges the
# worker role does not have. Unmapped, an insert carries exactly the
# granted columns and the database assigns the id.
task_events = Table(
    "task_events",
    metadata,
    Column("taskId", UUID(as_uuid=True), nullable=False),
    Column("type", String(32), nullable=False),
    Column("payload", Text, nullable=False),
    Column("created_at", DateTime, nullable=False),
)

# Event types, as the live stream has always named them.
EVENT_PROGRESS = "task-progress"
EVENT_LOG = "task-log"
EVENT_SUCCEEDED = "task-succeeded"
EVENT_FAILED = "task-failed"

# Why a task failed, for the UI (see the backend's jobs.py).
FAILURE_KIND_JOB = "worker_failure"

# Wakes the worker slots (trigger on INSERT of a PENDING task).
CHANNEL_TASK_QUEUED = "task_queued"
