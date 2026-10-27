"""Post-apply readiness check for apps that boot asynchronously.

``tofu apply`` returns as soon as OpenStack reports the instance as
ACTIVE, long before cloud-init has finished inside the VM. Apps that
need the platform to wait for their first-boot work (e.g. the
Kubernetes-App starting its student pods) declare an optional output::

    output "readiness_checks" {
      value = [{ instance_id = "...", name = "kurs", timeout_seconds = 1200 }]
    }

and end their first boot by writing exactly one line to the serial
console: ``APPSTORE-READY`` or ``APPSTORE-FAILED: <reason>``. The worker
reads the console via the OpenStack API (no network path to the VM, no
secrets) and fails the task on FAILED or when no marker shows up within
the instance's timeout. Apps without the output are not affected.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..utils.logger import LogCategory

if TYPE_CHECKING:
    from collections.abc import Callable

READY_MARKER = "APPSTORE-READY"
FAILED_MARKER = "APPSTORE-FAILED"

DEFAULT_TIMEOUT_SECONDS = 900
# Upper bound so a typo in an app can't hold a worker slot for hours;
# the apply itself is capped at 30 minutes as well.
MAX_TIMEOUT_SECONDS = 3600
POLL_INTERVAL_SECONDS = 15


@dataclass(frozen=True)
class ReadinessCheck:
    instance_id: str
    name: str
    timeout_seconds: int


def parse_readiness_checks(outputs: dict[str, Any] | None) -> list[ReadinessCheck]:
    """Extract the checks from raw ``tofu output -json`` data.

    Accepts a list of objects or a map ``name -> object``. Entries
    without an ``instance_id`` are skipped; a missing or malformed
    output yields an empty list, so a broken app output degrades to
    "no check" instead of failing an otherwise good deploy.
    """
    entry = (outputs or {}).get("readiness_checks")
    value = entry.get("value") if isinstance(entry, dict) else None
    if isinstance(value, dict):
        items = [{"name": key, **item} for key, item in value.items() if isinstance(item, dict)]
    elif isinstance(value, list):
        items = [item for item in value if isinstance(item, dict)]
    else:
        return []

    checks = []
    for item in items:
        instance_id = item.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id:
            continue
        try:
            timeout = int(item.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
        except (TypeError, ValueError):
            timeout = DEFAULT_TIMEOUT_SECONDS
        timeout = max(1, min(timeout, MAX_TIMEOUT_SECONDS))
        checks.append(ReadinessCheck(instance_id, str(item.get("name") or instance_id), timeout))
    return checks


def scan_console(console: str | None) -> tuple[str, str | None] | None:
    """Return ``("ready", None)``, ``("failed", reason)`` or ``None``.

    The last marker wins, so a re-run of the first-boot script after a
    failure can still report success.
    """
    for line in reversed((console or "").splitlines()):
        if FAILED_MARKER in line:
            reason = line.split(FAILED_MARKER, 1)[1].lstrip(": ").strip()
            return ("failed", reason or "no reason given")
        if READY_MARKER in line:
            return ("ready", None)
    return None


def wait_for_readiness(
    checks: list[ReadinessCheck],
    openstack: Any,
    task_logger: Any,
    *,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[str]:
    """Poll the serial console of every instance until each reports.

    Returns one error string per instance that reported FAILED or timed
    out; an empty list means all instances are ready.
    """
    start = clock()
    pending = {check.instance_id: check for check in checks}
    errors: list[str] = []
    for check in checks:
        task_logger.info(
            f"Waiting for {check.name} to report {READY_MARKER} (up to {check.timeout_seconds}s)",
            category=LogCategory.STATUS,
        )

    while pending:
        for instance_id, check in list(pending.items()):
            state = scan_console(openstack.console_log(instance_id))
            elapsed = int(clock() - start)
            if state and state[0] == "ready":
                task_logger.success(f"{check.name}: ready after {elapsed}s", category=LogCategory.STATUS)
                del pending[instance_id]
            elif state:
                errors.append(f"{check.name}: {state[1]}")
                task_logger.error(f"{check.name} reported a failed start: {state[1]}", category=LogCategory.ERROR)
                del pending[instance_id]
            elif elapsed >= check.timeout_seconds:
                errors.append(f"{check.name}: no {READY_MARKER} on the serial console after {check.timeout_seconds}s")
                task_logger.error(f"{check.name}: readiness timed out", category=LogCategory.ERROR)
                del pending[instance_id]
        if pending:
            sleep(poll_interval)
    return errors
