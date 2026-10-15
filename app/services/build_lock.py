"""Redis-backed distributed lock around the Packer image build.

Why this exists: the build phase in `tasks.py` does a check-then-act pair
(`check_image_exists` → `packer.build`) on the shared OpenStack Glance store.
Two parallel workers triggering a build of the same `(project_id, image_name)`
both observe "not found" and both kick off a build, leaving a duplicate image
behind and burning ~10 minutes of compute. This lock serializes the build for
a given image name within a given OpenStack project so only one worker
actually builds; the other re-checks Glance after waiting and skips straight
to OpenTofu if the image now exists.

Backend: Redis (already present as Celery's result backend, no new dep).

Lock semantics:

* ``SET NX PX`` with a unique token holds the lock with a short TTL
  (default 5 min). A daemon thread inside the holder periodically
  refreshes the TTL via ``PEXPIRE`` so a long-running build can't
  outlive its lease.
* Release uses a Lua-CAS so a worker can never release a lock that
  another worker grabbed after the first one's TTL expired.
* If the holding worker process is killed before ``release()`` runs
  (SIGKILL, OOM, container restart), the heartbeat thread dies with
  it and the lock self-heals after ``ttl_ms`` — bounded staleness.
"""

from __future__ import annotations

import threading
import time
import uuid

import redis

from ..config import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


_LUA_RELEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

_LUA_RENEW = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
"""

# Lock lease. Short enough that a crashed worker doesn't block the next
# deploy for long; the heartbeat below extends it on every tick so an
# in-flight build that takes longer than the lease still keeps the lock.
_DEFAULT_TTL_MS = 5 * 60 * 1000
# Heartbeat interval. Must be < TTL with margin so we don't miss a
# refresh window after a momentary GC pause / Redis hiccup.
_DEFAULT_HEARTBEAT_S = 60
_DEFAULT_POLL_S = 5
_DEFAULT_TOTAL_WAIT_S = 25 * 60


def _redis_client() -> redis.Redis:
    return redis.Redis.from_url(settings.CELERY_RESULT_BACKEND)


class PackerBuildLock:
    """Acquire a Redis lock keyed on (project, image_name).

    The class is a context-manager-shaped polling loop. Pattern:

        lock = PackerBuildLock(project_id, image_name)
        try:
            while True:
                held = lock.acquire_or_wait()
                if held:
                    if image_already_exists(): break
                    packer.build(...)
                    break
                if image_already_exists(): break
        finally:
            lock.release()
    """

    def __init__(
        self,
        project_id: str,
        image_name: str,
        *,
        ttl_ms: int = _DEFAULT_TTL_MS,
        heartbeat_interval_s: int = _DEFAULT_HEARTBEAT_S,
        poll_interval_s: int = _DEFAULT_POLL_S,
        total_wait_s: int = _DEFAULT_TOTAL_WAIT_S,
    ):
        self.key = f"lock:packer:{project_id or 'unknown'}:{image_name}"
        self.token = uuid.uuid4().hex
        self.ttl_ms = ttl_ms
        self.heartbeat_interval_s = heartbeat_interval_s
        self.poll_interval_s = poll_interval_s
        self.deadline = time.monotonic() + total_wait_s
        self._client = _redis_client()
        self._held = False
        # Heartbeat coordination. A daemon thread runs while the lock is
        # held and renews the TTL on every interval; ``_stop`` flips it
        # off during ``release()``. Daemon=True so the thread doesn't
        # keep the worker alive on shutdown.
        self._heartbeat: threading.Thread | None = None
        self._stop = threading.Event()

    # ----- acquisition ---------------------------------------------------

    def acquire_or_wait(self) -> bool:
        """Try to acquire. Returns True if held, False if we slept and the
        caller should re-check Glance and call again. Raises TimeoutError
        if the total wait budget is exhausted."""
        if self._client.set(self.key, self.token, nx=True, px=self.ttl_ms):
            self._held = True
            self._start_heartbeat()
            logger.info("Acquired Packer build lock", lock_key=self.key, ttl_ms=self.ttl_ms)
            return True
        if time.monotonic() > self.deadline:
            raise TimeoutError(
                f"Timed out waiting for Packer build lock {self.key} " f"(another worker is still building this image)"
            )
        # Look up the lock's remaining TTL so the operator/log reader has
        # a hint at how long the wait will be. ``pttl`` returns -2 if the
        # key has just expired (race with another waiter), -1 if no TTL
        # is set (shouldn't happen — we always set PX), or the remaining
        # ms otherwise.
        ttl_left_ms = self._client.pttl(self.key)
        logger.info(
            "Waiting for in-progress Packer build",
            lock_key=self.key,
            poll_interval_s=self.poll_interval_s,
            other_holder_ttl_ms=ttl_left_ms if ttl_left_ms and ttl_left_ms > 0 else None,
        )
        time.sleep(self.poll_interval_s)
        return False

    # ----- heartbeat -----------------------------------------------------

    def _start_heartbeat(self) -> None:
        """Spawn the renewal thread.

        The thread loops on ``_stop.wait(interval)`` so it sleeps
        cancellably and exits promptly when ``release()`` runs. CAS via
        the Lua script ensures we only renew our own lock — if another
        worker has somehow taken over after our TTL expired (e.g. our
        heartbeat fell behind a Redis outage), we don't accidentally
        steal their lease back.
        """
        self._stop.clear()
        thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"packer-lock-heartbeat-{self.key[-12:]}",
            daemon=True,
        )
        thread.start()
        self._heartbeat = thread

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            try:
                renewed = self._client.eval(_LUA_RENEW, 1, self.key, self.token, self.ttl_ms)
            except Exception as e:
                # Redis blip — try again next tick. We don't break the
                # loop on a single failure; if Redis stays down past
                # our TTL the lock will expire and another worker will
                # eventually pick up.
                logger.warning(f"Packer lock renewal raised: {e}")
                continue
            if not renewed:
                # Lock is gone (TTL expired faster than we could renew,
                # or someone manually deleted it). Stop heartbeating —
                # ``release()`` is a no-op in this case.
                logger.warning(
                    "Packer lock vanished mid-build; stopping heartbeat",
                    lock_key=self.key,
                )
                self._held = False
                return

    # ----- release -------------------------------------------------------

    def release(self) -> None:
        if not self._held:
            return
        # Stop the heartbeat first so it doesn't race with the delete.
        self._stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=2)
            self._heartbeat = None
        try:
            self._client.eval(_LUA_RELEASE, 1, self.key, self.token)
        except Exception as e:
            # Best-effort: even if release fails, the TTL will eventually
            # expire and the lock self-heals.
            logger.warning(f"Failed to release Packer lock {self.key}: {e}")
        finally:
            self._held = False

    # ----- context manager ergonomics -----------------------------------

    def __enter__(self) -> PackerBuildLock:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()
