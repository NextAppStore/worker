"""Per-task OpenStack credential materialization.

The worker receives an encrypted credential envelope inside the sealed job
payload (see backend `crud.openstack_credentials.get_dispatch_envelope`). This module
decrypts the envelope in-process and writes a `clouds.yaml` (mode 0600) into
the per-deployment workspace for the duration of one task. The file is
removed eagerly when the context manager exits — so, even on a crash, the
plaintext credential lives on disk only for the active phase of the build.

The plaintext is never logged. The `creds` dict is wiped from memory in
`__exit__` so the GC can reclaim it quickly.
"""

from __future__ import annotations

import base64
import contextlib
import os
from typing import Any

import yaml

from .. import job_context
from ..utils.crypto import decrypt


class CredentialEnvelopeError(Exception):
    """Raised when the dispatched envelope is missing fields or fails to decrypt."""


class PerTaskCloudsConfig:
    """Context manager: materialize a per-task `clouds.yaml`, then shred it.

    Usage:
        with PerTaskCloudsConfig(envelope, work_dir=repo_path) as env:
            packer.run(env_vars=env)
            tofu.run(env_vars=env)
    """

    # Profile name written into the per-task ``clouds.yaml``. App
    # OpenTofu templates reference this via ``provider "openstack" {
    # cloud = "openstack" }`` (matching the OpenStack convention used
    # in their docs), so changing this requires updating every
    # template — keep it as ``"openstack"`` unless there's a strong
    # reason to switch.
    CLOUD_NAME = "openstack"

    def __init__(self, envelope: dict[str, Any], work_dir: str):
        if not isinstance(envelope, dict):
            raise CredentialEnvelopeError("OpenStack credential envelope is missing or invalid")

        try:
            id_b64 = envelope["encrypted_identifier_b64"]
            secret_b64 = envelope["encrypted_secret_b64"]
            auth_type = envelope["auth_type"]
            auth_url = envelope["auth_url"]
        except KeyError as e:
            raise CredentialEnvelopeError(f"OpenStack credential envelope missing field: {e}")

        try:
            identifier = decrypt(base64.b64decode(id_b64.encode("ascii")))
            secret = decrypt(base64.b64decode(secret_b64.encode("ascii")))
        except Exception as e:
            # Don't surface InvalidToken text — only the type — to keep logs clean.
            raise CredentialEnvelopeError(
                f"Failed to decrypt OpenStack credential envelope ({type(e).__name__}). "
                "Check that backend and worker share the same CREDENTIAL_ENCRYPTION_KEY."
            )

        self._creds: dict[str, Any] | None = {
            "auth_type": auth_type,
            "auth_url": auth_url,
            "region_name": envelope.get("region_name"),
            "interface": envelope.get("interface") or "public",
            "identity_api_version": envelope.get("identity_api_version") or "3",
            "project_id": envelope.get("project_id"),
            "project_name": envelope.get("project_name"),
            "user_domain_name": envelope.get("user_domain_name"),
            "project_domain_name": envelope.get("project_domain_name") or envelope.get("user_domain_name"),
            "_identifier": identifier,
            "_secret": secret,
        }

        os.makedirs(work_dir, exist_ok=True)
        self.path = os.path.join(work_dir, "clouds.yaml")

    def _cloud_block(self) -> dict[str, Any]:
        c = self._creds
        if c is None:
            raise CredentialEnvelopeError("Credentials already shredded")

        block: dict[str, Any] = {
            "auth_type": c["auth_type"],
            "auth": {"auth_url": c["auth_url"]},
            "interface": c["interface"],
            "identity_api_version": c["identity_api_version"],
        }
        if c["region_name"]:
            block["region_name"] = c["region_name"]

        auth = block["auth"]
        if c["auth_type"] == "v3applicationcredential":
            auth["application_credential_id"] = c["_identifier"]
            auth["application_credential_secret"] = c["_secret"]
        else:
            auth["username"] = c["_identifier"]
            auth["password"] = c["_secret"]
            if c["project_id"]:
                auth["project_id"] = c["project_id"]
            if c["project_name"]:
                auth["project_name"] = c["project_name"]
            if c["user_domain_name"]:
                auth["user_domain_name"] = c["user_domain_name"]
            if c["project_domain_name"]:
                auth["project_domain_name"] = c["project_domain_name"]
        return block

    def _env_vars(self) -> dict[str, str]:
        """Mirror selected creds into OS_* env vars for tools that ignore clouds.yaml."""
        c = self._creds or {}
        env: dict[str, str] = {}
        if c.get("auth_url"):
            env["OS_AUTH_URL"] = c["auth_url"]
        if c.get("region_name"):
            env["OS_REGION_NAME"] = c["region_name"]
        if c.get("interface"):
            env["OS_INTERFACE"] = c["interface"]
        if c.get("identity_api_version"):
            env["OS_IDENTITY_API_VERSION"] = c["identity_api_version"]

        if c.get("auth_type") == "v3applicationcredential":
            env["OS_AUTH_TYPE"] = "v3applicationcredential"
            env["OS_APPLICATION_CREDENTIAL_ID"] = c["_identifier"]
            env["OS_APPLICATION_CREDENTIAL_SECRET"] = c["_secret"]
        else:
            env["OS_AUTH_TYPE"] = "password"
            env["OS_USERNAME"] = c["_identifier"]
            env["OS_PASSWORD"] = c["_secret"]
            if c.get("project_id"):
                env["OS_PROJECT_ID"] = c["project_id"]
            if c.get("project_name"):
                env["OS_PROJECT_NAME"] = c["project_name"]
            if c.get("user_domain_name"):
                env["OS_USER_DOMAIN_NAME"] = c["user_domain_name"]
            if c.get("project_domain_name"):
                env["OS_PROJECT_DOMAIN_NAME"] = c["project_domain_name"]
        return env

    def __enter__(self) -> dict[str, str]:
        # O_EXCL: refuse to overwrite a stale file from a previous task that
        # crashed before reaching its `__exit__`. mode 0600 is enforced by
        # the syscall, not by a separate chmod (no race window).
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            fd = os.open(self.path, flags, 0o600)
        except FileExistsError:
            # A stale file means a prior task in this same workspace died. Clean and retry.
            with contextlib.suppress(FileNotFoundError):
                os.remove(self.path)
            fd = os.open(self.path, flags, 0o600)

        try:
            with os.fdopen(fd, "w") as f:
                yaml.safe_dump(
                    {"clouds": {self.CLOUD_NAME: self._cloud_block()}},
                    f,
                    default_flow_style=False,
                )
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                os.remove(self.path)
            raise
        # Readable by the job slot's user (its tools load it), still 0600:
        # no other slot can read it.
        job_context.current().hand_over(self.path)

        env = {
            "OS_CLIENT_CONFIG_FILE": self.path,
            "OS_CLOUD": self.CLOUD_NAME,
        }
        env.update(self._env_vars())
        return env

    def __exit__(self, exc_type, exc, tb) -> None:
        # Shred file eagerly. The repo workspace cleanup in tasks.py:finally
        # would also remove it, but doing it here narrows the window.
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.path)
        # Drop plaintext references; let GC reclaim them quickly.
        self._creds = None
