"""
Terraform execution utilities with comprehensive structured logging.

Each long-running command (init, plan, apply, destroy) streams its
combined stdout/stderr line-by-line through an optional ``output_callback``
so a higher-level consumer (the deploy task's per-deployment logger) can
forward each line onto the Celery event bus while the command is still
running. The full output is also kept in memory and returned at the end
in the same ``(success, stdout, stderr)`` tuple as before, so existing
callers keep working unchanged.
"""

import contextlib
import json
import os
import subprocess
import threading
from collections.abc import Callable
from typing import Any

from ..config import settings
from ..utils.logger import LogCategory, get_logger

logger = get_logger(__name__)


# File written next to the cloned repo's terraform/ directory to force the
# `pg` backend regardless of what the upstream module declares. Terraform
# treats files ending in `_override.tf` as overrides and replaces the
# `terraform { backend ... }` block in base config with this one.
_PG_BACKEND_OVERRIDE_FILENAME = "pg_backend_override.tf"


def _pg_backend_override_hcl(schema_name: str) -> str:
    # schema_name is interpolated, not user-controlled (we generate it from
    # the deployment UUID). Quoted as an HCL string literal.
    safe = schema_name.replace('"', '\\"')
    return f'terraform {{\n  backend "pg" {{\n    schema_name = "{safe}"\n  }}\n}}\n'


OutputCallback = Callable[[str, str], None]
"""Signature: ``callback(tool_name, line) -> None``.

Invoked once per line read from the subprocess. ``tool_name`` lets the
callback distinguish ``terraform_init`` / ``terraform_plan`` / ... when one
callback is shared across operations.
"""


def var_flags(variables: dict[str, Any] | None) -> list[str]:
    """Render a variable map as ``-var key=value`` CLI arguments.

    Values arrive already encoded (see ``encode_terraform_vars`` /
    ``encode_packer_vars`` in ``app.tasks``), so nothing is re-encoded
    here — doing so a second time would double-escape JSON literals.
    Passing flag and value as separate argv entries means quoting inside
    an address or a JSON object reaches the tool unmodified.
    """
    flags: list[str] = []
    for key, value in (variables or {}).items():
        flags.extend(["-var", f"{key}={value}"])
    return flags


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
    caller and the live consumer see the lines in the order Terraform
    intended — Terraform interleaves progress and error messages across
    the two streams and separating them would scramble the chronology.
    The returned tuple still exposes the merged output as ``stdout`` and
    keeps ``stderr`` as an empty string for ABI compatibility.

    A timeout is enforced via ``Popen.wait(timeout)``; on expiry the
    process group is killed (children inherit the same group via
    ``start_new_session``) so terraform's child providers don't survive
    as orphans.
    """
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered — without this, output is held until
        # Terraform fills its 64 KiB pipe buffer
        env=env,
        start_new_session=True,
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
        # Kill the whole process group — terraform spawns provider plugins
        # as children and a plain process.kill() would orphan them.
        with contextlib.suppress(OSError, ProcessLookupError):
            os.killpg(process.pid, 9)
        reader.join(timeout=2)
        return 124, "\n".join(output_lines), "Timeout"

    reader.join(timeout=5)
    return returncode, "\n".join(output_lines), ""


class TerraformExecutor:
    """Executor for Terraform operations with detailed logging.

    When ``backend_conn_str`` and ``backend_schema_name`` are provided the
    executor configures Terraform's ``pg`` backend so state is persisted
    in a remote Postgres. The conn string lives only in the per-process
    env (``PG_CONN_STR``), never on the command line.

    ``output_callback`` is optional; if set, each line of subprocess output
    is fed to it as it arrives. The deploy task uses this to forward lines
    onto its per-deployment logger which then ships them to the backend
    via Celery custom events.
    """

    def __init__(
        self,
        working_dir: str,
        env_vars: dict[str, str] | None = None,
        backend_conn_str: str | None = None,
        backend_schema_name: str | None = None,
        output_callback: OutputCallback | None = None,
    ):
        self.working_dir = working_dir
        self.terraform_path = settings.TERRAFORM_PATH
        self.env_vars = env_vars or {}
        self.backend_conn_str = backend_conn_str
        self.backend_schema_name = backend_schema_name
        self.output_callback = output_callback

    def _get_env(self, extra_env: dict[str, str] | None = None) -> dict[str, str]:
        """Get environment variables including OpenStack credentials and Terraform debug logging."""
        env = os.environ.copy()
        env.update(self.env_vars)
        if extra_env:
            env.update(extra_env)
        # ``TF_LOG`` is honoured by the terraform CLI and emits an enormous
        # amount of provider/RPC trace to stderr — useful when debugging
        # the worker itself, but it drowns the human-readable error block
        # we forward to the user. Default to off; opt back in via
        # ``WORKER_TF_LOG`` if you really want it.
        tf_log = os.environ.get("WORKER_TF_LOG", "")
        if tf_log:
            env["TF_LOG"] = tf_log
        else:
            env.pop("TF_LOG", None)
        # PG_CONN_STR is read by Terraform's pg backend. Putting it in env
        # (not -backend-config="conn_str=...") keeps the password out of
        # the process listing and command logs.
        if self.backend_conn_str:
            env["PG_CONN_STR"] = self.backend_conn_str
        return env

    def _write_pg_backend_override(self) -> None:
        """Write ``pg_backend_override.tf`` so init configures the pg backend.

        No-op if no schema is configured (legacy local-state mode).
        """
        if not self.backend_schema_name:
            return
        override_path = os.path.join(self.working_dir, _PG_BACKEND_OVERRIDE_FILENAME)
        with open(override_path, "w") as f:
            f.write(_pg_backend_override_hcl(self.backend_schema_name))
        logger.debug(f"[TF] Wrote pg backend override at {override_path} (schema_name={self.backend_schema_name})")

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
        logger.debug(f"[TF] Running command: {' '.join(cmd)}")
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
        """Initialize Terraform in the working directory.

        Returns:
            tuple: (success, stdout, stderr)  — stderr merged into stdout
        """
        logger.operation_start("terraform_init", working_dir=self.working_dir)
        try:
            # Write the backend override BEFORE init runs. If we leave it
            # to a later step Terraform will already have committed to
            # whatever backend the upstream module declares (or to ``local``).
            self._write_pg_backend_override()

            cmd = [self.terraform_path, "init", "-input=false"]
            # ``-reconfigure`` makes init idempotent across deploy/update/destroy
            # runs on the same dir — the schema_name comes from the override
            # file we just wrote, not from a stale ``.terraform/terraform.tfstate``.
            if self.backend_schema_name:
                cmd.append("-reconfigure")

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_init", timeout=300)

            if stdout:
                logger.command_output("terraform_init", stdout, 0 if success else 1)

            if not success:
                logger.error(
                    "Terraform init failed",
                    category=LogCategory.ERROR,
                    stderr=stderr[:1000] if stderr else None,
                )
            else:
                logger.success("Terraform init completed", category=LogCategory.STATUS)

            logger.operation_end("terraform_init", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("Terraform init failed with exception", exception=e)
            logger.operation_end("terraform_init", success=False)
            return False, "", str(e)

    def plan(self, var_file: str | None = None, variables: dict[str, Any] | None = None) -> tuple[bool, str, str]:
        """Run terraform plan."""
        logger.operation_start("terraform_plan", var_file=var_file, var_count=len(variables or {}))
        try:
            # ``-lock=false``: each deployment has its own pg schema (see
            # ``_tfstate_schema_name`` in the worker), so state is isolated
            # per deployment. A shared state lock would serialize unrelated
            # deployments; real concurrency on the same state is already
            # prevented by the backend.
            cmd = [self.terraform_path, "plan", "-input=false", "-lock=false"]
            if var_file:
                cmd.extend(["-var-file", var_file])
            cmd.extend(var_flags(variables))

            logger.info("Analyzing Terraform configuration...", category=LogCategory.STATUS)
            logger.debug("plan variable keys", category=LogCategory.OPERATION, keys=list((variables or {}).keys()))

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_plan", timeout=300)

            if stdout:
                logger.command_output("terraform_plan", stdout, 0 if success else 1)

            if not success:
                logger.error("Terraform plan failed", category=LogCategory.ERROR)
            else:
                logger.success("Terraform plan completed", category=LogCategory.STATUS)

            logger.operation_end("terraform_plan", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("Terraform plan failed with exception", exception=e)
            logger.operation_end("terraform_plan", success=False)
            return False, "", str(e)

    def apply(
        self,
        var_file: str | None = None,
        variables: dict[str, Any] | None = None,
        targets: list[str] | None = None,
        replace: list[str] | None = None,
    ) -> tuple[bool, str, str]:
        """Run terraform apply.

        Args:
            var_file: Optional ``-var-file`` value.
            variables: Optional ``-var KEY=VALUE`` map.
            targets:  Optional list of resource addresses to pass via
                ``-target=…``. Used by the per-VM redeploy task to scope
                an apply to ONE compute instance — leaves the rest of
                the deployment untouched. Each entry MUST be a single
                terraform state address (``type.name[index]``); passing
                a malformed value would expand into a different CLI
                flag, so the worker is the line of defense (callers in
                the backend additionally whitelist against the state).
            replace: Optional list of resource addresses to pass via
                ``-replace=…``. Combined with ``targets`` for the
                redeploy contract: the targeted resource gets
                explicitly tainted so terraform destroys + recreates
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
            # ``-lock=false``: see the rationale in ``plan()`` — the
            # per-deployment schema isolates state.
            cmd = [self.terraform_path, "apply", "-auto-approve", "-input=false", "-lock=false"]
            if var_file:
                cmd.extend(["-var-file", var_file])
            cmd.extend(var_flags(variables))
            # Targets / replaces must be a plain list of addresses; we
            # don't validate the shape here (the caller has already
            # whitelisted against the cached TF state). Passing them as
            # separate ``[flag, value]`` pairs to subprocess avoids
            # shell quoting concerns — double-quotes inside the address
            # (``team_ide["Team-A"]``) reach Terraform unmodified.
            for tgt in targets or []:
                cmd.extend(["-target", tgt])
            for repl in replace or []:
                cmd.extend(["-replace", repl])

            logger.info("Applying Terraform configuration (this may take minutes)...", category=LogCategory.STATUS)

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_apply", timeout=1800)

            if stdout:
                logger.command_output("terraform_apply", stdout, 0 if success else 1)

            if not success:
                logger.error("Terraform apply failed", category=LogCategory.ERROR)
            else:
                logger.success("Terraform apply completed successfully", category=LogCategory.STATUS)

            logger.operation_end("terraform_apply", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("Terraform apply failed with exception", exception=e)
            logger.operation_end("terraform_apply", success=False)
            return False, "", str(e)

    def destroy(
        self,
        var_file: str | None = None,
        variables: dict[str, Any] | None = None,
        refresh: bool = True,
    ) -> tuple[bool, str, str]:
        """Run terraform destroy.

        ``refresh=False`` adds ``-refresh=false`` so Terraform tears down
        purely from state without re-reading data sources. Only used as a
        targeted fallback by the worker when a stale data source (e.g. a
        Glance image deleted out-of-band) blocks the refresh-based destroy.
        """
        logger.operation_start("terraform_destroy", var_file=var_file, var_count=len(variables or {}))
        try:
            # ``-lock=false``: see the rationale in ``plan()`` — the
            # per-deployment schema isolates state.
            cmd = [self.terraform_path, "destroy", "-auto-approve", "-input=false", "-lock=false"]
            if not refresh:
                cmd.append("-refresh=false")
            if var_file:
                cmd.extend(["-var-file", var_file])
            cmd.extend(var_flags(variables))

            logger.info("Destroying Terraform resources (this may take minutes)...", category=LogCategory.STATUS)

            success, stdout, stderr = self._run_streamed(cmd, tool_name="terraform_destroy", timeout=1800)

            if stdout:
                logger.command_output("terraform_destroy", stdout, 0 if success else 1)

            if not success:
                logger.error("Terraform destroy failed", category=LogCategory.ERROR)
            else:
                logger.success("Terraform destroy completed successfully", category=LogCategory.STATUS)

            logger.operation_end("terraform_destroy", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("Terraform destroy failed with exception", exception=e)
            logger.operation_end("terraform_destroy", success=False)
            return False, "", str(e)

    # ------------------------------------------------------------------
    # Short read-only operations (still buffered — fast, no live consumer
    # waits on these)
    # ------------------------------------------------------------------

    def output(self) -> dict[str, Any] | None:
        """Get terraform outputs as JSON."""
        logger.operation_start("terraform_output")
        try:
            cmd = [self.terraform_path, "output", "-json"]
            logger.debug(f"[TF] Running command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd, cwd=self.working_dir, capture_output=True, text=True, timeout=60, env=self._get_env()
            )
            if result.returncode != 0:
                logger.warning(
                    "Terraform output retrieval failed", category=LogCategory.OPERATION, returncode=result.returncode
                )
                logger.operation_end("terraform_output", success=False)
                return None

            outputs = json.loads(result.stdout)
            logger.success(f"Terraform outputs retrieved ({len(outputs)} outputs)", category=LogCategory.STATUS)
            logger.operation_end("terraform_output", success=True)
            return outputs
        except Exception as e:
            logger.exception("Terraform output extraction failed with exception", exception=e)
            logger.operation_end("terraform_output", success=False)
            return None

    def state_pull(self) -> str | None:
        """Return the current state JSON via ``terraform state pull``.

        Works for both the local backend (reads ``terraform.tfstate``) and
        the pg backend (reads from Postgres). Used by the worker to
        snapshot state onto the task row for debugging — the canonical
        copy lives in the pg backend.
        """
        try:
            cmd = [self.terraform_path, "state", "pull"]
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=60,
                env=self._get_env(),
            )
            if result.returncode != 0:
                logger.warning(
                    "Terraform state pull failed",
                    category=LogCategory.OPERATION,
                    returncode=result.returncode,
                )
                return None
            return result.stdout
        except Exception as e:
            logger.warning(f"Terraform state pull raised: {e}", category=LogCategory.OPERATION)
            return None
