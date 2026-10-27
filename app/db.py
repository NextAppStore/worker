"""Database access for the worker process (never for the jobs it runs).

psycopg 3 throughout: SQLAlchemy sessions for claiming and recording, and
plain psycopg connections for ``LISTEN task_queued`` (see runner.py) and
the Packer build lock.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from .config import settings


def sqlalchemy_url(url: str) -> str:
    """``url`` with the psycopg (3) driver selected (compose files carry plain ``postgresql://``)."""
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql":
        return url
    return parsed.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)


def libpq_url(url: str) -> str:
    """``url`` without a SQLAlchemy driver suffix, for plain psycopg connections."""
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql":
        return url
    return parsed.set(drivername="postgresql").render_as_string(hide_password=False)


# Per running job: one connection for its events, one for its lease, one
# for a Packer lock; plus the claims. The LISTEN connections are separate.
engine = create_engine(
    sqlalchemy_url(settings.DATABASE_URL),
    pool_pre_ping=True,
    pool_size=2 * settings.WORKER_CONCURRENCY + 1,
    max_overflow=settings.WORKER_CONCURRENCY + 2,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
