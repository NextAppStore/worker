"""
Packer execution utilities with comprehensive structured logging.

``init`` and ``build`` stream their output line-by-line via
``_stream_subprocess`` so the deploy task's per-deployment logger can ship
each line onto the Celery event bus while the build is still running.
``validate`` is short and stays as a buffered call.
"""

import json
import os
import subprocess
from typing import Any

from ..config import settings
from ..utils.logger import LogCategory, get_logger
from .terraform_executor import OutputCallback, _stream_subprocess

logger = get_logger(__name__)


class PackerExecutor:
    """Executor for Packer operations with detailed logging.

    ``output_callback`` is optional; when set, each line of subprocess
    output is fed to it as it arrives. Used by the deploy task to forward
    Packer build output onto the Celery event bus during long builds.
    """

    def __init__(
        self,
        working_dir: str,
        env_vars: dict[str, str] | None = None,
        output_callback: OutputCallback | None = None,
    ):
        self.working_dir = working_dir
        self.packer_path = settings.PACKER_PATH
        self.env_vars = env_vars or {}
        self.output_callback = output_callback

    def _get_env(self, extra_env: dict[str, str] | None = None) -> dict[str, str]:
        """Get environment variables including OpenStack credentials and Packer debug logging."""
        env = os.environ.copy()
        env.update(self.env_vars)
        if extra_env:
            env.update(extra_env)
        env["PACKER_LOG"] = "1"
        return env

    def init(self) -> tuple[bool, str, str]:
        """Initialize Packer (install required plugins). Streams output."""
        logger.operation_start("packer_init", working_dir=self.working_dir)
        try:
            cmd = [self.packer_path, "init", "."]
            logger.debug(f"[Packer] Running command: {' '.join(cmd)}")
            returncode, stdout, stderr = _stream_subprocess(
                cmd,
                cwd=self.working_dir,
                env=self._get_env(),
                timeout=300,
                tool_name="packer_init",
                output_callback=self.output_callback,
            )
            success = returncode == 0

            if stdout:
                logger.command_output("packer_init", stdout, returncode)

            if not success:
                if returncode == 124:
                    logger.error("Packer init timed out after 5 minutes", category=LogCategory.ERROR)
                else:
                    logger.error(
                        "Packer init failed",
                        category=LogCategory.ERROR,
                        returncode=returncode,
                    )
            else:
                logger.success("Packer init completed", category=LogCategory.STATUS)

            logger.operation_end("packer_init", success)
            return success, stdout, stderr
        except Exception as e:
            logger.exception("Packer init failed with exception", exception=e)
            logger.operation_end("packer_init", success=False)
            return False, "", str(e)

    def validate(self, template_file: str, variables: dict[str, Any] | None = None) -> tuple[bool, str, str]:
        """Validate a Packer template (short, buffered)."""
        logger.operation_start("packer_validate", template=template_file)
        logger.debug(f"Packer working directory: {self.working_dir}", category=LogCategory.SYSTEM)
        try:
            cmd = [self.packer_path, "validate"]
            if variables:
                for key, value in variables.items():
                    value_str = json.dumps(value) if isinstance(value, list | dict) else str(value)
                    cmd.extend(["-var", f"{key}={value_str}"])
            cmd.append(".")

            logger.debug(f"[Packer] Running command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd, cwd=self.working_dir, capture_output=True, text=True, timeout=60, env=self._get_env()
            )
            success = result.returncode == 0

            if result.stdout:
                logger.command_output("packer_validate", result.stdout, result.returncode)

            if not success:
                error_msg = self._extract_error_from_packer(result.stderr)
                logger.error(
                    f"Packer validation failed: {error_msg}",
                    category=LogCategory.ERROR,
                    returncode=result.returncode,
                    stderr=result.stderr[:1000],
                )
            else:
                logger.success("Packer template validated", category=LogCategory.STATUS)

            logger.operation_end("packer_validate", success)
            return success, result.stdout, result.stderr
        except Exception as e:
            logger.exception("Packer validation failed with exception", exception=e)
            logger.operation_end("packer_validate", success=False)
            return False, "", str(e)

    def build(
        self,
        template_file: str,
        variables: dict[str, Any] | None = None,
        extra_env: dict[str, str] | None = None,
        force: bool = True,
    ) -> tuple[bool, str]:
        """Build a Packer image. Streams output line-by-line."""
        logger.operation_start("packer_build", template=template_file, force=force, var_count=len(variables or {}))
        try:
            cmd = [self.packer_path, "build"]
            if force:
                cmd.append("-force")

            if variables:
                for key, value in variables.items():
                    value_str = json.dumps(value) if isinstance(value, list | dict) else str(value)
                    cmd.extend(["-var", f"{key}={value_str}"])
            cmd.append(".")

            logger.info(
                "Starting Packer build process (this may take several minutes)...",
                category=LogCategory.STATUS,
            )
            logger.debug(f"[Packer] Running command: {' '.join(cmd)}")

            returncode, stdout, _ = _stream_subprocess(
                cmd,
                cwd=self.working_dir,
                env=self._get_env(extra_env),
                timeout=3600,
                tool_name="packer_build",
                output_callback=self.output_callback,
            )
            success = returncode == 0
            output_lines_count = stdout.count("\n") + 1 if stdout else 0

            if not success:
                if returncode == 124:
                    logger.error("Packer build timed out after 1 hour", category=LogCategory.ERROR)
                else:
                    logger.error(
                        "Packer build failed",
                        category=LogCategory.ERROR,
                        returncode=returncode,
                        output_lines=output_lines_count,
                    )
            else:
                logger.success(
                    "Packer build completed successfully",
                    category=LogCategory.STATUS,
                    output_lines=output_lines_count,
                )

            logger.operation_end("packer_build", success)
            return success, stdout
        except Exception as e:
            logger.exception("Packer build failed with exception", exception=e)
            logger.operation_end("packer_build", success=False)
            return False, str(e)

    def _extract_error_from_packer(self, stderr: str) -> str:
        """Extract a meaningful error message from Packer stderr.

        Packer stderr contains a lot of TRACE/DEBUG noise; we filter that
        and pull out lines that look like actual errors.
        """
        lines = stderr.split("\n")
        errors = []
        for line in lines:
            if "[TRACE]" in line or "[DEBUG]" in line or "plugingetter" in line:
                continue
            if "* Get" in line or "Error" in line or "error" in line or line.strip().startswith("*"):
                errors.append(line.strip())

        if errors:
            return " | ".join(errors[:3])

        for line in reversed(lines):
            if line.strip() and "[TRACE]" not in line and "[DEBUG]" not in line:
                return line.strip()
        return "Unknown error"
