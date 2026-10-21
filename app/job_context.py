"""Where and how a job's tools run: time limit, user, environment.

An app's OpenTofu code comes from a Git repository we do not control; a
``local-exec`` provisioner runs whatever it likes inside the worker. Before,
every tool inherited ``os.environ`` — the Fernet key, the broker URL and the
connection string of the tfstate database holding *every* deployment's
state. So the tools (OpenTofu, Packer, the OpenStack CLI) now get:

- **a minimal environment**: ``PATH``, ``HOME``, the OpenStack variables of
  the job's own credential and the state backend's address and per-task
  token. Nothing of the worker's own configuration (``DATABASE_URL``,
  ``CREDENTIAL_ENCRYPTION_KEY``, ``GIT_ACCESS_TOKEN``) is passed on.
- **their own user** when ``WORKER_JOB_UID_BASE`` is set (the image does):
  slot *n* runs its jobs' tools as UID ``base + n``. A job then cannot read
  the worker's ``/proc/<pid>/environ``, nor the files of a job running in
  another slot (another user's ``clouds.yaml``). The worker itself runs as
  root for that and drops to the slot's UID only for the child processes.
- **a deadline**: every subprocess gets at most the time left until the
  job's limit (``WORKER_JOB_TIMEOUT_SECONDS``), so a hanging provider cannot
  hold a worker slot forever.

The runner binds a context per job with :func:`bind`; code running outside
a job (tests, one-off calls) gets an unrestricted default — still with the
minimal environment.
"""

from __future__ import annotations

import contextlib
import os
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .config import settings

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

_FALLBACK_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


class JobTimeout(Exception):
    """The job ran out of time."""


@dataclass(frozen=True)
class JobContext:
    """Limits and identity for the tools of one job; see the module docstring."""

    # time.monotonic() value after which no tool may run any more.
    deadline: float = float("inf")
    # UID/GID the tools run as; None means "as the worker itself".
    uid: int | None = None
    home: str = field(default_factory=lambda: os.path.join(settings.TEMP_REPO_BASE_PATH, "home"))

    def remaining(self) -> float:
        """Seconds left until the deadline (negative once passed)."""
        return self.deadline - time.monotonic()

    def timeout(self, limit: float) -> float:
        """``limit`` capped to the time left. Raises JobTimeout when none is left."""
        left = self.remaining()
        if left <= 0:
            raise JobTimeout(f"job time limit of {settings.WORKER_JOB_TIMEOUT_SECONDS}s reached")
        return min(limit, left)

    def env(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        """The complete environment of a tool: the minimum plus ``extra``."""
        env = {
            "PATH": os.environ.get("PATH") or _FALLBACK_PATH,
            "HOME": self.home,
            "LANG": "C.UTF-8",
            # OpenTofu/Packer: no update checks, no interactive prompts.
            "CHECKPOINT_DISABLE": "1",
            "TF_IN_AUTOMATION": "1",
            "TF_INPUT": "0",
        }
        if extra:
            env.update(extra)
        return env

    def popen_kwargs(self) -> dict[str, Any]:
        """Extra ``subprocess`` arguments that run the child as the slot's user."""
        if self.uid is None:
            return {}
        return {"user": self.uid, "group": self.uid, "extra_groups": [], "umask": 0o077}

    def hand_over(self, path: str) -> None:
        """Give ``path`` (recursively) to the slot's user, so its tools can use it."""
        if self.uid is None:
            return
        os.chown(path, self.uid, self.uid)
        if os.path.isdir(path):
            # Private to the slot: the neighbouring slots are other users.
            os.chmod(path, 0o700)
        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                os.chown(os.path.join(root, name), self.uid, self.uid, follow_symlinks=False)

    def write_private(self, path: str, content: str) -> None:
        """Write a file only the job's tools (and the worker) can read."""
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(content)
        if self.uid is not None:
            os.chown(path, self.uid, self.uid)


_current: ContextVar[JobContext | None] = ContextVar("job_context", default=None)


def current() -> JobContext:
    """The context of the job running in this thread, or an unrestricted default."""
    return _current.get() or JobContext()


def for_slot(slot: int) -> JobContext:
    """A fresh context for a job starting now in worker slot ``slot``."""
    deadline = time.monotonic() + settings.WORKER_JOB_TIMEOUT_SECONDS
    if settings.WORKER_JOB_UID_BASE is None:
        return JobContext(deadline=deadline)
    uid = settings.WORKER_JOB_UID_BASE + slot
    # A home per slot, owned by the slot's user: plugin caches survive from
    # one job to the next, but no two users share one.
    home = os.path.join(settings.WORKER_JOB_HOME_BASE, f"slot-{slot}")
    os.makedirs(home, mode=0o700, exist_ok=True)
    os.chown(home, uid, uid)
    return JobContext(deadline=deadline, uid=uid, home=home)


@contextlib.contextmanager
def bind(ctx: JobContext) -> Iterator[JobContext]:
    """Make ``ctx`` the current context for the duration of the ``with`` block."""
    token = _current.set(ctx)
    try:
        yield ctx
    finally:
        _current.reset(token)
