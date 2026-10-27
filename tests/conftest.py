"""Pytest configuration and fixtures."""

import os
import shutil
import tempfile
import uuid
from pathlib import Path

# The app reads its settings at import time; unit tests never connect, they
# only need a syntactically valid URL. Integration tests use TEST_DATABASE_URL.
os.environ.setdefault("DATABASE_URL", "postgresql://worker:worker@localhost:5432/unused")
os.environ.setdefault("CREDENTIAL_ENCRYPTION_KEY", "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")

import pytest  # noqa: E402


# ----------------------------------------------------------------
# Auto-marker policy
# ----------------------------------------------------------------
# CI rennt zwei getrennte Lanes: ``pytest -m unit`` und
# ``pytest -m integration``. Tests, die *gar keinen* Marker tragen,
# fallen aus **beiden** Selektoren raus — d.h. sie würden lautlos
# nicht in CI ausgeführt, obwohl sie lokal grün sind. Genau das ist
# uns hier passiert: 12 von 23 Tests waren unmarked und wurden nie
# durch die Pipeline laufen.
#
# Diese Hook setzt für jeden Test ohne ``integration``/``slow``-
# Marker implizit ``unit``. Damit gilt: **default ist unit**, und
# nur Tests, die echte externe Abhängigkeiten brauchen (DB, Broker,
# Netz), müssen explizit als ``integration`` markiert werden.
def pytest_collection_modifyitems(config, items):
    for item in items:
        markers = {m.name for m in item.iter_markers()}
        # ``integration`` / ``slow`` haben Vorrang — wer explizit
        # markiert, will nicht in die unit-Lane gezogen werden.
        if "integration" not in markers and "slow" not in markers and "unit" not in markers:
            item.add_marker(pytest.mark.unit)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    if temp_path.exists():
        shutil.rmtree(temp_path)


@pytest.fixture
def mock_git_url():
    """Mock Git URL for testing."""
    return "https://github.com/test-org/test-repo.git"


@pytest.fixture
def mock_tag():
    """Mock Git tag for testing."""
    return "v1.0.0"


# ----------------------------------------------------------------
# Database for integration tests
# ----------------------------------------------------------------
# The queue tables belong to the backend (its migrations create them). The
# worker's tests build the part the worker sees from ``app/job_tables.py``
# plus the backend's enum types and notification triggers, in a throw-away
# database. The backend's ``tests/test_worker_contract.py`` checks the same
# statements against the real migrations and the worker role.
QUEUE_TEST_DDL = """
CREATE TYPE taskstatus AS ENUM ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED');
CREATE TYPE tasktype AS ENUM ('DEPLOY', 'UPDATE', 'DESTROY', 'PAUSE', 'RESUME', 'REDEPLOY');
"""

# As the backend creates it: the id (stream cursor) is not part of the
# worker's view in app/job_tables.py, so the table comes from SQL here.
TASK_EVENTS_TEST_DDL = """
CREATE TABLE task_events (
    id BIGSERIAL PRIMARY KEY,
    "taskId" UUID NOT NULL REFERENCES tasks ("taskId") ON DELETE CASCADE,
    type VARCHAR(32) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
"""

QUEUE_TEST_TRIGGERS = """
CREATE FUNCTION appstore_notify_task_queued() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_notify('task_queued', ''); RETURN NULL; END $$;
CREATE TRIGGER tasks_notify_queued AFTER INSERT ON tasks
    FOR EACH ROW WHEN (NEW.status = 'PENDING') EXECUTE FUNCTION appstore_notify_task_queued();
CREATE FUNCTION appstore_notify_task_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_notify('task_events', (SELECT "deploymentId"::text FROM tasks WHERE "taskId" = NEW."taskId"));
RETURN NULL; END $$;
CREATE TRIGGER task_events_notify AFTER INSERT ON task_events
    FOR EACH ROW EXECUTE FUNCTION appstore_notify_task_event();
"""


# Mirror of the backend migration f8a9b0c1d2e3_worker_role: the worker's code
# under test runs as a role with exactly these privileges, so a statement
# that needs more (e.g. an implicit ``RETURNING id``) fails here, not in prod.
WORKER_ROLE_GRANTS = """
GRANT USAGE ON SCHEMA public TO {role};
GRANT SELECT ("taskId", "deploymentId", type, status, payload, claimed_by, created_at) ON tasks TO {role};
GRANT UPDATE (status, payload, claimed_by, lease_until, started_at, finished_at, logs, outputs,
              current_phase, progress_pct) ON tasks TO {role};
GRANT INSERT ("taskId", type, payload, created_at) ON task_events TO {role};
GRANT USAGE ON SEQUENCE task_events_id_seq TO {role};
"""


@pytest.fixture(scope="session")
def queue_db():
    """A fresh database with the queue tables and a worker role; dropped afterwards.

    Yields ``(admin_url, worker_url)`` (libpq URLs). Skips when
    ``TEST_DATABASE_URL`` is not set (the unit lane has no database).
    """
    admin_url = os.environ.get("TEST_DATABASE_URL")
    if not admin_url:
        pytest.skip("TEST_DATABASE_URL not set")
    import psycopg
    from psycopg import sql
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url

    from app.db import libpq_url, sqlalchemy_url
    from app.job_tables import metadata, tasks

    admin = libpq_url(admin_url)
    suffix = uuid.uuid4().hex[:10]
    name, role = f"worker_test_{suffix}", f"worker_role_{suffix}"
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'worker'").format(sql.Identifier(role)))
    db_admin_url = make_url(admin).set(database=name).render_as_string(hide_password=False)
    worker_url = (
        make_url(admin).set(database=name, username=role, password="worker").render_as_string(hide_password=False)
    )
    try:
        with psycopg.connect(db_admin_url, autocommit=True) as conn:
            conn.execute(QUEUE_TEST_DDL)
        engine = create_engine(sqlalchemy_url(db_admin_url))
        metadata.create_all(engine, tables=[tasks])
        engine.dispose()
        with psycopg.connect(db_admin_url, autocommit=True) as conn:
            conn.execute(TASK_EVENTS_TEST_DDL)
            conn.execute(QUEUE_TEST_TRIGGERS)
            conn.execute(WORKER_ROLE_GRANTS.format(role=role))
        yield db_admin_url, worker_url
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


@pytest.fixture(scope="session")
def queue_db_url(queue_db):
    """libpq URL of the queue test database, connecting as the worker role."""
    return queue_db[1]


def _factory(url):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.db import sqlalchemy_url

    engine = create_engine(sqlalchemy_url(url), pool_size=10, max_overflow=10)
    return engine, sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@pytest.fixture
def queue_admin_factory(queue_db):
    """Sessions as the database owner, for test setup and assertions; empties the tables afterwards."""
    from sqlalchemy import text

    engine, factory = _factory(queue_db[0])
    yield factory
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE task_events, tasks RESTART IDENTITY"))
    engine.dispose()


@pytest.fixture
def queue_session_factory(queue_db, queue_admin_factory):
    """Sessions as the worker role — what the code under test gets."""
    engine, factory = _factory(queue_db[1])
    yield factory
    engine.dispose()
