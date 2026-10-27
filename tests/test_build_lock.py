"""The Postgres advisory lock around Packer builds (``app/services/build_lock.py``).

Each lock holds its own connection, like two jobs in two workers would.
"""

from __future__ import annotations

import threading
import time

import pytest

from app.services.build_lock import PackerBuildLock


@pytest.mark.unit
class TestKeyNaming:
    def test_key_includes_project_id_and_image(self):
        assert PackerBuildLock("my-proj", "ubuntu-22").key == "lock:packer:my-proj:ubuntu-22"

    @pytest.mark.parametrize("project", [None, ""])
    def test_key_uses_unknown_without_project(self, project):
        assert PackerBuildLock(project, "img").key == "lock:packer:unknown:img"

    def test_release_without_acquire_is_a_noop(self):
        lock = PackerBuildLock("p", "i")
        lock.release()
        lock.release()
        assert not lock.held


@pytest.mark.integration
class TestAgainstPostgres:
    def test_second_job_cannot_take_a_held_lock(self, queue_db_url):
        with (
            PackerBuildLock("p", "img", conninfo=queue_db_url) as a,
            PackerBuildLock("p", "img", conninfo=queue_db_url) as b,
        ):
            assert a.try_acquire() is True
            assert b.try_acquire() is False
            # A different image of the same project builds in parallel.
            with PackerBuildLock("p", "other", conninfo=queue_db_url) as c:
                assert c.try_acquire() is True

    def test_waiter_blocks_until_the_holder_releases(self, queue_db_url):
        holder = PackerBuildLock("p", "img", conninfo=queue_db_url)
        assert holder.try_acquire()
        waiter = PackerBuildLock("p", "img", conninfo=queue_db_url, wait_timeout_s=10)
        acquired_at: list[float] = []

        def wait() -> None:
            waiter.acquire()
            acquired_at.append(time.monotonic())

        t = threading.Thread(target=wait)
        t.start()
        time.sleep(0.5)
        assert acquired_at == []  # still blocked in Postgres
        released_at = time.monotonic()
        holder.release()
        t.join(timeout=5)
        assert acquired_at and acquired_at[0] - released_at < 1.0
        assert waiter.held
        waiter.release()

    def test_lock_of_a_dead_holder_is_freed_by_postgres(self, queue_db_url):
        """A worker killed mid-build loses its connection, and with it the lock."""
        holder = PackerBuildLock("p", "img", conninfo=queue_db_url)
        assert holder.try_acquire()
        holder._conn.close()  # the process died: no unlock ever runs
        with PackerBuildLock("p", "img", conninfo=queue_db_url, wait_timeout_s=5) as waiter:
            waiter.acquire()
            assert waiter.held

    def test_waiting_too_long_raises_timeout(self, queue_db_url):
        with PackerBuildLock("p", "img", conninfo=queue_db_url) as holder:
            assert holder.try_acquire()
            with PackerBuildLock("p", "img", conninfo=queue_db_url, wait_timeout_s=1) as waiter:
                started = time.monotonic()
                with pytest.raises(TimeoutError):
                    waiter.acquire()
                assert 0.8 < time.monotonic() - started < 5
                assert not waiter.held
