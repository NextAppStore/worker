"""
OpenStack service for image management
"""

import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


class OpenStackService:
    """Service for OpenStack image and compute-instance operations.

    All methods shell out to the ``openstack`` CLI (python-openstackclient)
    rather than using the SDK in-process, mirroring how the rest of the
    worker invokes external tooling (Packer, OpenTofu). Exit-code zero
    is treated as success; stderr is captured and returned to the
    caller for surfacing in the per-deployment log.

    The CLI is configured via env vars (OS_AUTH_URL etc.) supplied by
    :class:`PerTaskCloudsConfig`. ``OS_CLIENT_CONFIG_FILE`` and
    ``OS_CLOUD`` make the CLI prefer the per-task ``clouds.yaml`` over
    any ambient configuration on the worker host.
    """

    def __init__(self, env_vars: dict[str, str] | None = None):
        """
        Initialize OpenStack service

        Args:
            env_vars: OpenStack environment variables (OS_AUTH_URL, OS_USERNAME, etc.)
        """
        self.env_vars = env_vars or {}

    def _run(self, args: list[str], timeout: int = 60) -> tuple[int, str, str]:
        """Run an ``openstack`` CLI command with the configured env vars.

        Centralised so every call carries the per-task credentials and
        the same error handling. Returns the raw triple
        ``(returncode, stdout, stderr)`` so callers can decide how to
        report; structured-output methods JSON-decode stdout themselves.
        """
        env = os.environ.copy()
        env.update(self.env_vars)
        try:
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )
            return (result.returncode, result.stdout or "", result.stderr or "")
        except subprocess.TimeoutExpired:
            return (-1, "", f"Timeout after {timeout}s running: {' '.join(args)}")
        except FileNotFoundError:
            return (-1, "", "OpenStack CLI not found — install python-openstackclient")
        except Exception as e:  # noqa: BLE001 — surfaced to user
            return (-1, "", f"Error running openstack CLI: {e}")

    def check_image_exists(self, image_name: str) -> tuple[bool, str | None]:
        """
        Check if an image with the given name already exists in OpenStack

        Args:
            image_name: Name of the image to check

        Returns:
            tuple: (exists: bool, image_id: str | None)
        """
        if not self.env_vars.get("OS_AUTH_URL"):
            logger.warning("No OpenStack credentials available for image check")
            return (False, None)

        rc, stdout, stderr = self._run(
            ["openstack", "image", "list", "--name", image_name, "-f", "json"],
            timeout=30,
        )
        if rc != 0:
            logger.error(f"Failed to check image existence: {stderr}")
            return (False, None)

        try:
            images = json.loads(stdout)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse OpenStack CLI output: {e}")
            return (False, None)

        if images:
            image_id = images[0].get("ID")
            logger.info(f"Image '{image_name}' already exists with ID: {image_id}")
            return (True, image_id)
        logger.info(f"Image '{image_name}' does not exist")
        return (False, None)

    # ------------------------------------------------------------------
    # Compute instance lifecycle (used by pause / resume)
    # ------------------------------------------------------------------
    #
    # ``server stop`` / ``server start`` are idempotent at the CLI
    # level: stopping an already-SHUTOFF instance returns exit code 0
    # with no error, and the same applies to starting an already-ACTIVE
    # instance. This makes pause/resume safe to retry without extra
    # state checks. We surface the CLI's stderr verbatim so the user
    # can see the precise reason on the rare hard failure (locked task,
    # auth glitch, etc.).

    def server_show(self, server_id: str) -> dict | None:
        """Return the parsed ``openstack server show <id> -f json`` payload.

        Used to log the human-readable name and current power state
        before issuing a stop/start, so the per-deployment log shows
        ``"web-1 (ACTIVE) → stopping"`` rather than just a UUID.
        Returns ``None`` if the server can't be fetched (deleted,
        permission, transient failure).
        """
        rc, stdout, stderr = self._run(
            ["openstack", "server", "show", server_id, "-f", "json"],
            timeout=30,
        )
        if rc != 0:
            logger.warning(f"server show failed for {server_id}: {stderr.strip()}")
            return None
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            return None

    def console_log(self, server_id: str, lines: int = 500) -> str | None:
        """Return the last ``lines`` lines of the instance's serial console.

        Used by the post-apply readiness check (see
        :mod:`app.services.readiness`): apps write a status marker to
        the serial console, which Nova exposes without any network path
        to the VM. Returns ``None`` if the log can't be fetched (instance
        still building, console not available, transient API failure);
        callers treat that as "not ready yet" and poll again.
        """
        rc, stdout, stderr = self._run(
            ["openstack", "console", "log", "show", "--lines", str(lines), server_id],
            timeout=30,
        )
        if rc != 0:
            logger.debug(f"console log show failed for {server_id}: {stderr.strip()}")
            return None
        return stdout

    def server_stop(self, server_id: str) -> tuple[bool, str | None]:
        """Stop an OpenStack compute instance.

        Returns ``(True, None)`` on success, ``(False, stderr)`` on
        failure. The CLI is idempotent on already-SHUTOFF instances —
        no extra status check needed.
        """
        rc, _stdout, stderr = self._run(
            ["openstack", "server", "stop", server_id],
            timeout=120,
        )
        if rc == 0:
            return (True, None)
        return (False, stderr.strip() or "openstack server stop failed")

    def server_start(self, server_id: str) -> tuple[bool, str | None]:
        """Start an OpenStack compute instance.

        Returns ``(True, None)`` on success, ``(False, stderr)`` on
        failure. Idempotent on already-ACTIVE instances.
        """
        rc, _stdout, stderr = self._run(
            ["openstack", "server", "start", server_id],
            timeout=120,
        )
        if rc == 0:
            return (True, None)
        return (False, stderr.strip() or "openstack server start failed")
