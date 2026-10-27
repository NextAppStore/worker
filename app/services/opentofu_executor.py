"""
OpenTofu execution utilities with comprehensive structured logging.

Each long-running command (init, plan, apply, destroy) streams its
combined stdout/stderr line-by-line through an optional ``output_callback``
so a higher-level consumer (the job's per-deployment logger) can write each
line as a task event while the command is still running. The full output is
also kept in memory and returned at the end in the same
``(success, stdout, stderr)`` tuple.

Every command runs in the job's context (``job_context``): a minimal
environment, the slot's user and the job's remaining time. The state lives
in the backend API (``/internal/tfstate``), reached through OpenTofu's
``http`` backend with a per-task token.
"""

import contextlib
import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .. import job_context
from ..config import settings
from ..utils.crypto import encrypt
from ..utils.logger import LogCategory, get_logger

logger = get_logger(__name__)


# Written into the cloned repo's terraform/ directory to force the ``http``
# backend regardless of what the upstream module declares. OpenTofu treats
# files ending in ``_override.tf`` as overrides and replaces the
# ``terraform { backend ... }`` block of the base config with this one. The
# address and credentials come from TF_HTTP_* environment variables, so
# nothing secret is written to disk.
_BACKEND_OVERRIDE_FILENAME = "appstore_backend_override.tf"
_BACKEND_OVERRIDE_HCL = 'terraform {\n  backend "http" {}\n}\n'

# What OpenTofu writes into the working directory when it cannot save the
# state to the backend after an apply/destroy.
_ERRORED_STATE_FILENAME = "errored.tfstate"


@dataclass(frozen=True)
class StateBackend:
    """Where OpenTofu keeps the state of one deployment, and the key to it."""

    address: str
    username: str
    password: str

    @classmethod
    def for_job(cls, deployment_id: str, task_id: str, token: str) -> "StateBackend":
        """The API's ``/internal/tfstate/<deployment>`` endpoint, authenticated as the task.

        ``token`` is the per-task state token from the sealed job payload; it
        only works while the task is RUNNING and only for its deployment.
        """
        base = settings.APPSTORE_API_URL.rstrip("/")
        return cls(address=f"{base}/internal/tfstate/{deployment_id}", username=task_id, password=token)

    def env(self) -> dict[str, str]:
        """``TF_HTTP_*`` variables that configure OpenTofu's http backend (state, locking, retries)."""
        return {
            "TF_HTTP_ADDRESS": self.address,
            "TF_HTTP_LOCK_ADDRESS": self.address,
            "TF_HTTP_UNLOCK_ADDRESS": self.address,
            "TF_HTTP_LOCK_METHOD": "LOCK",
            "TF_HTTP_UNLOCK_METHOD": "UNLOCK",
            "TF_HTTP_USERNAME": self.username,
            "TF_HTTP_PASSWORD": self.password,
            # Ride out a backend restart (see config.WORKER_STATE_RETRY_*).
            "TF_HTTP_RETRY_MAX": str(settings.WORKER_STATE_RETRY_MAX),
            "TF_HTTP_RETRY_WAIT_MIN": "1",
            "TF_HTTP_RETRY_WAIT_MAX": str(settings.WORKER_STATE_RETRY_WAIT_MAX),
        }

    @property
    def deployment_id(self) -> str:
        return self.address.rstrip("/").rsplit("/", 1)[-1]


OutputCallback = Callable[[str, str], None]
"""Signature: ``callback(tool_name, line) -> None``.

Invoked once per line read from the subprocess. ``tool_name`` lets the
callback distinguish ``terraform_init`` / ``terraform_plan`` / ... when one
callback is shared across operations.
"""


def _stream_subprocess(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str],
    timeout: int,
    tool_name: str,
    output_callback: OutputCallback | None,
) -> tuple[int, str, str]:
    """Run a subprocess and stream its output line-by-line.

    stdout and stderr are merged onto stdout (``stderr=STDOUT``) so the
    caller and the live consumer see the lines in the order OpenTofu
    intended — OpenTofu interleaves progress and error messages across
    the two streams and separating them would scramble the chronology.
    The returned tuple still exposes the merged output as ``stdout`` and
    keeps ``stderr`` as an empty string for ABI compatibility.

    A timeout is enforced via ``Popen.wait(timeout)``, capped at the time
    the job has left; on expiry the process group is killed (children
    inherit the same group via ``start_new_session``) so OpenTofu's child
    providers don't survive as orphans. The child runs as the job slot's
    user (``job_context``).
    """
    ctx = job_context.current()
    timeout = ctx.timeout(timeout)
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered — without this, output is held until
        # OpenTofu fills its 64 KiB pipe buffer
        env=env,
        start_new_session=True,
        **ctx.popen_kwargs(),
    )
    output_lines: list[str] = []

    def _drain_stdout() -> None:
        # Read in a thread so the parent can enforce a wall-clock timeout
        # without blocking on readline.
        try:
            assert process.stdout is not None
            for raw in process.stdout:
                line = raw.rstrip("\n")
                output_lines.append(line)
                if output_callback is not None:
                    with contextlib.suppress(Exception):
                        # Never let a flaky live-stream callback break the
                        # actual deployment; swallow and keep draining.
                        output_callback(tool_name, line)
        except Exception:
            pass

    reader = threading.Thread(target=_drain_stdout, name=f"{tool_name}-reader", daemon=True)
    reader.start()

    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Kill the whole process group — OpenTofu spawns provider plugins
        # as children and a plain process.kill() would orphan them.
        with contextlib.suppress(OSError, ProcessLookupError):
            os.killpg(process.pid, 9)
        reader.join(timeout=2)
        return 124, "\n".join(output_lines), "Timeout"

    reader.join(timeout=5)
    return returncode, "\n".join(output_lines), ""


class OpenTofuExecutor:
    """Executor for OpenTofu operations with detailed logging.

    With a ``state_backend`` the executor configures OpenTofu's ``http``
    backend so the state lives in the backend API. Its address and per-task
    token travel only in the tool's environment (``TF_HTTP_*``), never on
    the command line or on disk. Without one, OpenTofu uses local state
    (tests only).

    ``output_callback`` is optional; if set, each line of subprocess output
    is fed to it as it arrives. The jobs use this to forward lines onto
    their per-deployment logger, which writes them as task events.
    """

    def __init__(
        self,
        working_dir: str,
        env_vars: dict[str, str] | None = None,
        state_backend: StateBackend | None = None,
        output_callback: OutputCallback | None = None,
    ):
        self.working_dir = working_dir
        self.tofu_path = settings.TOFU_PATH
        self.env_vars = env_vars or {}
        self.state_backend = state_backend
        self.output_callback = output_callback

    def _get_env(self, extra_env: dict[str, str] | None = None) -> dict[str, str]:
        """The tool's whole environment: the job minimum, the job's credentials, the state backend.

        Nothing is inherited from the worker (see ``job_context``).
        """
        extra = dict(self.env_vars)
        if extra_env:
            extra.update(extra_env)
        if self.state_backend:
            extra.update(self.state_backend.env())
        # ``TF_LOG`` emits an enormous amount of provider/RPC trace —
        # useful when debugging the worker itself, but it drowns the
        # human-readable error block we forward to the user. Off unless
        # ``WORKER_TF_LOG`` is set.
        if settings.WORKER_TF_LOG:
            extra["TF_LOG"] = settings.WORKER_TF_LOG
        return job_context.current().env(extra)

    def _write_backend_override(self) -> None:
        """Write the http-backend override file (only with a state backend)."""
        if not self.state_backend:
            return
        override_path = os.path.join(self.working_dir, _BACKEND_OVERRIDE_FILENAME)
        job_context.current().write_private(override_path, _BACKEND_OVERRIDE_HCL)
        logger.debug(f"[Tofu] Wrote http backend override at {override_path}")

    # ------------------------------------------------------------------
    # Long-running operations (streamed)
    # ------------------------------------------------------------------

    def _run_streamed(
        self,
        cmd: list[str],
        *,
        tool_name: str,
        timeout: int,
    ) -> tuple[bool, str, str]:
        logger.debug(f"[Tofu] Running command: {' '.join(cmd)}")
        returncode, stdout, stderr = _stream_subprocess(
            cmd,
            cwd=self.working_dir,
            env=self._get_env(),
            timeout=timeout,
            tool_name=tool_name,
            output_callback=self.output_callback,
        )
        return returncode == 0, stdout, stderr

    def init(self) -> tuple[bool, str, str]:
        """Initialize OpenTofu in the working directory.

        Returns:
            tuple: (success, stdout, stderr)  — stderr merged into stdout
        """
        logger.operation_start("terraform_init", working_dir=self.working_dir)
        try:
            # Write the backend override BEFORE init runs. If we leave it
            # to a later step OpenTofu will already have committed to
            # whatever backend the upstream module declares (or to ``local``).
            self._write_backend_override()

            cmd = [self.tofu_path, "init", "-input=false"]
            # ``-reconfigure`` makes init idempotent across runs on the same
            # dir — the backend comes from the override file we just wrote,
            # not from a stale ``.terraform/terraform.tfstate``.
            if self.state_backend:
                cmd.append("-reconfigure")

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_init", timeout=300)

            if stdout:
                logger.command_output("terraform_init", stdout, 0 if success else 1)

            if not success:
                logger.error(
                    "OpenTofu init failed",
                    category=LogCategory.ERROR,
                    stderr=stderr[:1000] if stderr else None,
                )
            else:
                logger.success("OpenTofu init completed", category=LogCategory.STATUS)

            logger.operation_end("terraform_init", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("OpenTofu init failed with exception", exception=e)
            logger.operation_end("terraform_init", success=False)
            return False, "", str(e)

    def plan(self, var_file: str | None = None, variables: dict[str, Any] | None = None) -> tuple[bool, str, str]:
        """Run tofu plan."""
        logger.operation_start("terraform_plan", var_file=var_file, var_count=len(variables or {}))
        try:
            # The http backend locks the deployment's state for the run (the
            # API holds the lock per task, see backend tf_state_store).
            cmd = [self.tofu_path, "plan", "-input=false"]
            if var_file:
                cmd.extend(["-var-file", var_file])
            if variables:
                for key, value in variables.items():
                    cmd.extend(["-var", f"{key}={value}"])

            logger.info("Analyzing OpenTofu configuration...", category=LogCategory.STATUS)
            logger.debug("plan variable keys", category=LogCategory.OPERATION, keys=list((variables or {}).keys()))

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_plan", timeout=300)

            if stdout:
                logger.command_output("terraform_plan", stdout, 0 if success else 1)

            if not success:
                logger.error("OpenTofu plan failed", category=LogCategory.ERROR)
            else:
                logger.success("OpenTofu plan completed", category=LogCategory.STATUS)

            logger.operation_end("terraform_plan", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("OpenTofu plan failed with exception", exception=e)
            logger.operation_end("terraform_plan", success=False)
            return False, "", str(e)

    def apply(
        self,
        var_file: str | None = None,
        variables: dict[str, Any] | None = None,
        targets: list[str] | None = None,
        replace: list[str] | None = None,
    ) -> tuple[bool, str, str]:
        """Run tofu apply.

        Args:
            var_file: Optional ``-var-file`` value.
            variables: Optional ``-var KEY=VALUE`` map.
            targets:  Optional list of resource addresses to pass via
                ``-target=…``. Used by the per-VM redeploy task to scope
                an apply to ONE compute instance — leaves the rest of
                the deployment untouched. Each entry MUST be a single
                OpenTofu state address (``type.name[index]``); passing
                a malformed value would expand into a different CLI
                flag, so the worker is the line of defense (callers in
                the backend additionally whitelist against the state).
            replace: Optional list of resource addresses to pass via
                ``-replace=…``. Combined with ``targets`` for the
                redeploy contract: the targeted resource gets
                explicitly tainted so OpenTofu destroys + recreates
                it instead of detecting "no changes" and short-
                circuiting. Same validation rules as ``targets``.
        """
        logger.operation_start(
            "terraform_apply",
            var_file=var_file,
            var_count=len(variables or {}),
            target_count=len(targets or []),
            replace_count=len(replace or []),
        )
        try:
            cmd = [self.tofu_path, "apply", "-auto-approve", "-input=false"]
            if var_file:
                cmd.extend(["-var-file", var_file])
            if variables:
                for key, value in variables.items():
                    cmd.extend(["-var", f"{key}={value}"])
            # Targets / replaces must be a plain list of addresses; we
            # don't validate the shape here (the caller has already
            # whitelisted against the cached TF state). Passing them as
            # separate ``[flag, value]`` pairs to subprocess avoids
            # shell quoting concerns — double-quotes inside the address
            # (``team_ide["Team-A"]``) reach OpenTofu unmodified.
            for tgt in targets or []:
                cmd.extend(["-target", tgt])
            for repl in replace or []:
                cmd.extend(["-replace", repl])

            logger.info("Applying OpenTofu configuration (this may take minutes)...", category=LogCategory.STATUS)

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_apply", timeout=1800)
            self._rescue_errored_state()

            if stdout:
                logger.command_output("terraform_apply", stdout, 0 if success else 1)

            if not success:
                logger.error("OpenTofu apply failed", category=LogCategory.ERROR)
            else:
                logger.success("OpenTofu apply completed successfully", category=LogCategory.STATUS)

            logger.operation_end("terraform_apply", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("OpenTofu apply failed with exception", exception=e)
            logger.operation_end("terraform_apply", success=False)
            return False, "", str(e)

    def destroy(
        self,
        var_file: str | None = None,
        variables: dict[str, Any] | None = None,
        refresh: bool = True,
    ) -> tuple[bool, str, str]:
        """Run tofu destroy.

        ``refresh=False`` adds ``-refresh=false`` so OpenTofu tears down
        purely from state without re-reading data sources. Only used as a
        targeted fallback by the worker when a stale data source (e.g. a
        Glance image deleted out-of-band) blocks the refresh-based destroy.
        """
        logger.operation_start("terraform_destroy", var_file=var_file, var_count=len(variables or {}))
        try:
            cmd = [self.tofu_path, "destroy", "-auto-approve", "-input=false"]
            if not refresh:
                cmd.append("-refresh=false")
            if var_file:
                cmd.extend(["-var-file", var_file])
            if variables:
                for key, value in variables.items():
                    cmd.extend(["-var", f"{key}={value}"])

            logger.info("Destroying OpenTofu resources (this may take minutes)...", category=LogCategory.STATUS)

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_destroy", timeout=1800)
            self._rescue_errored_state()

            if stdout:
                logger.command_output("terraform_destroy", stdout, 0 if success else 1)

            if not success:
                logger.error("OpenTofu destroy failed", category=LogCategory.ERROR)
            else:
                logger.success("OpenTofu destroy completed successfully", category=LogCategory.STATUS)

            logger.operation_end("terraform_destroy", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("OpenTofu destroy failed with exception", exception=e)
            logger.operation_end("terraform_destroy", success=False)
            return False, "", str(e)

    # ------------------------------------------------------------------
    # State that could not be saved
    # ------------------------------------------------------------------

    def _notice(self, line: str) -> None:
        """A line for the task's live log (via the output callback) and the worker log."""
        logger.warning(line, category=LogCategory.WARNING)
        if self.output_callback is not None:
            with contextlib.suppress(Exception):
                self.output_callback("state_rescue", line)

    def _rescue_errored_state(self) -> None:
        """Deliver ``errored.tfstate`` if OpenTofu left one; never lose it.

        OpenTofu writes this file when it could not save the state to the
        backend even after its own retries — the backend was unreachable
        for minutes at the end of an apply/destroy. The resources it lists
        exist in OpenStack, so the file must not be deleted with the job
        directory. An encrypted copy goes to ``WORKER_STATE_RESCUE_DIR`` (a
        volume) right away, so it survives even if the worker dies while
        retrying. The worker then keeps pushing the file (same task, same
        token; the backend lets a task override its own leftover lock) for
        up to ``WORKER_STATE_RESCUE_SECONDS`` and drops the copy once that
        worked; otherwise the task log says where the copy is and how to
        put it back.

        The OpenTofu command itself has already failed either way, so the
        task ends FAILED; this only makes sure the state survives.
        """
        path = os.path.join(self.working_dir, _ERRORED_STATE_FILENAME)
        if not self.state_backend or not os.path.exists(path):
            return
        # Keep the encrypted copy first: if the worker dies while retrying
        # below, the copy on the volume is what survives, not the job dir.
        kept = self._preserve(path)
        self._notice(
            "OpenTofu could not save the state to the AppStore API; "
            f"it was kept in errored.tfstate (encrypted safety copy on the worker: {kept}). "
            "Retrying the upload…"
        )
        deadline = time.monotonic() + settings.WORKER_STATE_RESCUE_SECONDS
        delay = 5.0
        while True:
            try:
                returncode, _, _ = _stream_subprocess(
                    [self.tofu_path, "state", "push", _ERRORED_STATE_FILENAME],
                    cwd=self.working_dir,
                    env=self._get_env(),
                    timeout=120,
                    tool_name="state_rescue",
                    output_callback=self.output_callback,
                )
            except job_context.JobTimeout:
                break
            if returncode == 0:
                for done in (path, kept):
                    with contextlib.suppress(OSError):
                        os.remove(done)
                self._notice("State saved to the AppStore API after all (errored.tfstate pushed).")
                return
            if time.monotonic() + delay > deadline:
                break
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        self._notice(
            f"State could NOT be saved; an encrypted copy is kept on the worker at {kept}. "
            "Restore it with backend `python scripts/restore_tfstate.py "
            f"{self.state_backend.deployment_id} <file>` before changing this deployment again."
        )

    def _preserve(self, path: str) -> str:
        """Copy ``path`` encrypted (Fernet, root-only) into the rescue directory; return the new path."""
        os.makedirs(settings.WORKER_STATE_RESCUE_DIR, mode=0o700, exist_ok=True)
        # A mounted volume comes up 0755; only the worker (root) reads it.
        os.chmod(settings.WORKER_STATE_RESCUE_DIR, 0o700)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        assert self.state_backend is not None
        name = f"{self.state_backend.deployment_id}_{self.state_backend.username}_{stamp}.tfstate.enc"
        target = os.path.join(settings.WORKER_STATE_RESCUE_DIR, name)
        with open(path) as f:
            sealed = encrypt(f.read())
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(sealed)
        return target

    # ------------------------------------------------------------------
    # Short read-only operations (still buffered — fast, no live consumer
    # waits on these)
    # ------------------------------------------------------------------

    def output(self) -> dict[str, Any] | None:
        """Get tofu outputs as JSON."""
        logger.operation_start("terraform_output")
        try:
            cmd = [self.tofu_path, "output", "-json"]
            logger.debug(f"[Tofu] Running command: {' '.join(cmd)}")
            ctx = job_context.current()
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=ctx.timeout(60),
                env=self._get_env(),
                **ctx.popen_kwargs(),
            )
            if result.returncode != 0:
                logger.warning(
                    "OpenTofu output retrieval failed", category=LogCategory.OPERATION, returncode=result.returncode
                )
                logger.operation_end("terraform_output", success=False)
                return None

            outputs = json.loads(result.stdout)
            logger.success(f"OpenTofu outputs retrieved ({len(outputs)} outputs)", category=LogCategory.STATUS)
            logger.operation_end("terraform_output", success=True)
            return outputs
        except Exception as e:
            logger.exception("OpenTofu output extraction failed with exception", exception=e)
            logger.operation_end("terraform_output", success=False)
            return None

    def state_pull(self) -> str | None:
        """Return the current state JSON via ``tofu state pull``.

        Works for both the local backend (reads ``terraform.tfstate``) and
        the http backend (reads from the API). Pause/resume use it to find
        the deployment's compute instances.
        """
        try:
            cmd = [self.tofu_path, "state", "pull"]
            ctx = job_context.current()
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=ctx.timeout(60),
                env=self._get_env(),
                **ctx.popen_kwargs(),
            )
            if result.returncode != 0:
                logger.warning(
                    "OpenTofu state pull failed",
                    category=LogCategory.OPERATION,
                    returncode=result.returncode,
                )
                return None
            return result.stdout
        except Exception as e:
            logger.warning(f"OpenTofu state pull raised: {e}", category=LogCategory.OPERATION)
            return None


def pending_rescued_states() -> list[str]:
    """Encrypted states in the rescue directory that were never delivered.

    A copy is removed once its upload worked, so whatever is left here
    needs ``restore_tfstate.py`` in the backend (the worker logs it on start).
    """
    try:
        names = sorted(os.listdir(settings.WORKER_STATE_RESCUE_DIR))
    except FileNotFoundError:
        return []
    return [os.path.join(settings.WORKER_STATE_RESCUE_DIR, n) for n in names if n.endswith(".tfstate.enc")]
