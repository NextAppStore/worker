"""Postgres advisory lock around the Packer image build.

Why this exists: the build phase in ``tasks.py`` does a check-then-act pair
(``check_image_exists`` → ``packer.build``) on the shared OpenStack Glance
store. Two parallel jobs building the same ``(project_id, image_name)``
would both observe "not found" and both build, leaving a duplicate image
and burning ~10 minutes of compute. This lock serializes the build for a
given image name within a given OpenStack project: only one job builds,
the others wait and then re-check Glance.

A session-level advisory lock on a connection of its own, held for the
build. Waiting blocks inside Postgres (``pg_advisory_lock`` with a
``lock_timeout``) instead of polling. If the holding worker dies, its
connection ends and Postgres releases the lock at once — no TTL, no
heartbeat thread, nothing to expire. (Before: Redis ``SET NX`` with a TTL,
a renewal thread, Lua compare-and-delete and a 5-second sleep loop.)

Usage (see ``tasks._build_one_packer_image``)::

    with PackerBuildLock(project_id, image_name) as lock:
        if not lock.try_acquire():
            log("another job builds this image, waiting")
            lock.acquire()          # blocks; TimeoutError after wait_timeout_s
            if image_exists(): ...  # built by the other job
        build()
"""

from __future__ import annotations

import psycopg
from psycopg import errors

from ..config import settings
from ..db import libpq_url
from ..utils.logger import get_logger

logger = get_logger(__name__)

# Advisory lock namespace for Packer builds, so these keys can never meet
# the backend's per-user (single-key) or per-deployment (namespace 1) locks.
_LOCK_NAMESPACE = 2
_DEFAULT_TOTAL_WAIT_S = 25 * 60


class PackerBuildLock:
    """Advisory lock keyed on (project, image_name), held on its own connection."""

    def __init__(
        self,
        project_id: str | None,
        image_name: str,
        *,
        wait_timeout_s: int = _DEFAULT_TOTAL_WAIT_S,
        conninfo: str | None = None,
    ):
        self.key = f"lock:packer:{project_id or 'unknown'}:{image_name}"
        self.wait_timeout_s = wait_timeout_s
        self._conninfo = conninfo
        self._conn: psycopg.Connection | None = None
        self.held = False

    def _connection(self) -> psycopg.Connection:
        if self._conn is None:
            self._conn = psycopg.connect(self._conninfo or libpq_url(settings.DATABASE_URL), autocommit=True)
        return self._conn

    def try_acquire(self) -> bool:
        """Take the lock if it is free. Returns whether it is held now."""
        row = (
            self._connection()
            .execute("SELECT pg_try_advisory_lock(%s, hashtext(%s))", (_LOCK_NAMESPACE, self.key))
            .fetchone()
        )
        self.held = bool(row and row[0])
        if self.held:
            logger.info("Acquired Packer build lock", lock_key=self.key)
        return self.held

    def acquire(self) -> None:
        """Block until the lock is free and take it.

        Raises TimeoutError when the holder keeps it longer than
        ``wait_timeout_s`` (Postgres ``lock_timeout``).
        """
        conn = self._connection()
        logger.info("Waiting for in-progress Packer build", lock_key=self.key, timeout_s=self.wait_timeout_s)
        try:
            conn.execute(f"SET lock_timeout = '{int(self.wait_timeout_s)}s'")
            conn.execute("SELECT pg_advisory_lock(%s, hashtext(%s))", (_LOCK_NAMESPACE, self.key))
        except errors.LockNotAvailable as e:
            raise TimeoutError(
                f"Timed out waiting for Packer build lock {self.key} (another worker is still building this image)"
            ) from e
        finally:
            conn.execute("RESET lock_timeout")
        self.held = True
        logger.info("Acquired Packer build lock after waiting", lock_key=self.key)

    def release(self) -> None:
        """Release the lock and close its connection (idempotent).

        Closing the connection alone would release a session lock too; the
        explicit unlock just makes it immediate and visible in logs.
        """
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            if self.held:
                conn.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (_LOCK_NAMESPACE, self.key))
        except Exception as e:
            # Closing the connection below ends the session, and the lock with it.
            logger.warning(f"Failed to release Packer lock {self.key}: {e}")
        finally:
            self.held = False
            conn.close()

    def __enter__(self) -> PackerBuildLock:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()
