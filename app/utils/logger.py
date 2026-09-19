"""Structured logging for deployment tasks.

Three concerns are kept separate here:

* **Buffer** — every log entry is appended to an in-memory list so the worker
  task can hand the whole transcript back to the backend on completion.
* **Console sink** — entries are also rendered once to the standard Python
  logger so they appear in the worker container's stdout. Rendering lives in
  a dedicated sink so the buffer and the Python logger stay independent.
* **Event sink** — an optional callable that ships every entry to a
  downstream consumer. The deploy task wires the Celery event bus here so
  the backend's listener can stream entries to the browser.

A single ``StructuredLogger`` orchestrates these. The class is threadsafe:
``Popen`` line readers may write from a reader thread while the main task
thread also writes phase markers, so buffer access goes through an ``RLock``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# ----------------------------------------------------------------------------
# Public enums
# ----------------------------------------------------------------------------


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogCategory(StrEnum):
    PHASE = "phase"
    OPERATION = "operation"
    SYSTEM = "system"
    STATUS = "status"
    OUTPUT = "output"
    ERROR = "error"
    DEBUG = "debug"
    PROGRESS = "progress"
    # Kept for backwards compatibility with call sites that pass
    # ``category=LogCategory.WARNING``. Treated identically to SYSTEM in
    # rendering — the level (``warning``) carries the actual severity.
    WARNING = "warning"


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

_ANSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
_LEVEL_ICONS: dict[LogLevel, str] = {
    LogLevel.DEBUG: "🔍",
    LogLevel.INFO: "ℹ️",
    LogLevel.SUCCESS: "✓",
    LogLevel.WARNING: "⚠️",
    LogLevel.ERROR: "❌",
}


def _now_iso() -> str:
    """Current UTC time as ISO-8601 with explicit ``Z`` suffix."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def clean_text(text: str) -> str:
    """Strip ANSI escapes and trim whitespace."""
    return _ANSI_RE.sub("", text).strip()


def truncate_text(text: str, max_lines: int = 50, max_chars: int = 5000) -> str:
    """Truncate text intelligently for the buffered transcript.

    For tool output the *tail* almost always matters more than the
    head — Terraform/Packer print the error block last, and naively
    cutting off the end (``text[:max_chars]``) systematically hides
    the very thing we wanted to log. So the strategy is:

    * Line-cap first: if there are more lines than ``max_lines``, keep
      the first 15 and the last ``max_lines - 16`` so the trailing
      error block survives.
    * Char-cap second, but split the budget between head and tail
      (40% / 60%) instead of dropping the tail entirely.
    """
    lines = text.split("\n")
    if len(lines) > max_lines:
        tail_keep = max_lines - 16
        lines = lines[:15] + [f"... (truncated {len(lines) - 15 - tail_keep} lines) ..."] + lines[-tail_keep:]
        text = "\n".join(lines)

    if len(text) > max_chars:
        head_budget = max_chars * 4 // 10
        tail_budget = max_chars - head_budget - 64  # leave room for the marker
        head = text[:head_budget]
        tail = text[-tail_budget:]
        text = head + f"\n... (truncated {len(text) - head_budget - tail_budget} characters) ...\n" + tail

    return text


def _scalar_or_str(value: Any) -> Any:
    """Coerce one context value into a JSON-friendly scalar.

    Tools like ``orjson`` would handle anything; we default to the stdlib
    ``json`` module for portability, so we down-cast unknown types to their
    repr.
    """
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _scalar_or_str(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_scalar_or_str(v) for v in value]
    return str(value)


# ----------------------------------------------------------------------------
# LogEntry
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class LogEntry:
    """A single log entry with explicit slots for common metadata.

    Putting ``stdout``/``stderr``/``tool``/``phase`` etc. on the dataclass
    instead of leaving everything in a generic context dict means consumers
    (the frontend, the backend listener, the JSON exporter) can render those
    fields specifically — e.g. monospace blocks for tool output rather than a
    bag of strings inside ``extra``.
    """

    timestamp: str
    level: LogLevel
    category: LogCategory
    message: str
    correlation_id: str | None = None
    operation: str | None = None
    duration_ms: float | None = None
    tool: str | None = None
    returncode: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    phase: str | None = None
    progress_pct: int | None = None
    streaming: bool = False
    truncated: bool = False
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Flat JSON-serialisable representation; ``None`` fields are skipped."""
        d: dict[str, Any] = {
            "timestamp": self.timestamp,
            "level": self.level.value,
            "category": self.category.value,
            "message": self.message,
        }
        for key in (
            "correlation_id",
            "operation",
            "duration_ms",
            "tool",
            "returncode",
            "stdout",
            "stderr",
            "phase",
            "progress_pct",
        ):
            value = getattr(self, key)
            if value is not None:
                d[key] = round(value, 2) if key == "duration_ms" else value
        if self.streaming:
            d["streaming"] = True
        if self.truncated:
            d["truncated"] = True
        if self.extra:
            # Merge into top-level so consumers see context fields flat on
            # the entry dict.
            for k, v in self.extra.items():
                if k not in d:
                    d[k] = v
        return d

    def __str__(self) -> str:
        icon = _LEVEL_ICONS.get(self.level, "•")
        duration = f" [{self.duration_ms:.2f}ms]" if self.duration_ms is not None else ""
        return f"{icon} [{self.timestamp}] {self.message}{duration}"


# ----------------------------------------------------------------------------
# Sinks
# ----------------------------------------------------------------------------


class _Buffer:
    """Threadsafe append-only list of entries.

    ``RLock`` because export methods (``get_logs_dict``) iterate while a
    second thread (``Popen`` reader) may still be appending. Using ``list``
    + lock is plenty for our throughput; a queue/deque would be overkill.
    """

    def __init__(self) -> None:
        self._entries: list[LogEntry] = []
        self._lock = threading.RLock()

    def append(self, entry: LogEntry) -> None:
        with self._lock:
            self._entries.append(entry)

    def snapshot(self) -> list[LogEntry]:
        with self._lock:
            return list(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


class _ConsoleSink:
    """Write each entry to the standard Python logger exactly once.

    Disabled via ``WORKER_LOG_CONSOLE=0`` — useful in tests where pytest's
    capture and our own buffer would otherwise both grow.
    """

    def __init__(self, name: str, enabled: bool) -> None:
        self._logger = logging.getLogger(name)
        self._enabled = enabled
        self._method: dict[LogLevel, Callable[[str], None]] = {
            LogLevel.DEBUG: self._logger.debug,
            LogLevel.INFO: self._logger.info,
            LogLevel.SUCCESS: self._logger.info,
            LogLevel.WARNING: self._logger.warning,
            LogLevel.ERROR: self._logger.error,
        }

    def write(self, entry: LogEntry) -> None:
        if not self._enabled:
            return
        self._method.get(entry.level, self._logger.info)(str(entry))


EventEmitter = Callable[[str, dict[str, Any]], None]
"""Signature: ``emit(event_name, payload_dict) -> None``.

The deploy task plugs Celery's ``self.send_event`` here so every log entry
also turns into a ``task-log`` event on the bus. ``None`` disables it.
"""


# ----------------------------------------------------------------------------
# StructuredLogger
# ----------------------------------------------------------------------------


class StructuredLogger:
    """Buffer + console + optional event-bus emitter for one deployment task."""

    LOG_EVENT_NAME = "task-log"
    PROGRESS_EVENT_NAME = "task-progress"

    def __init__(
        self,
        name: str,
        *,
        correlation_id: str | None = None,
        console: bool = True,
        event_emitter: EventEmitter | None = None,
        track_timing: bool = True,
    ) -> None:
        self.name = name
        self.correlation_id = correlation_id
        self.track_timing = track_timing
        self._buffer = _Buffer()
        self._console = _ConsoleSink(name, console and os.environ.get("WORKER_LOG_CONSOLE", "1") != "0")
        self._event_emitter = event_emitter
        self._operation_stack: list[tuple[str, float]] = []
        self._stack_lock = threading.Lock()

    # ----- emitter wiring (settable post-construction) --------------------

    def set_event_emitter(self, emitter: EventEmitter | None) -> None:
        """Attach or detach the Celery-event sink at runtime.

        ``tasks.py`` constructs the logger before it can capture ``self`` for
        ``send_event``, so we need a setter.
        """
        self._event_emitter = emitter

    # ----- core write-path ------------------------------------------------

    def _record(self, entry: LogEntry, *, emit_event: bool = True) -> None:
        self._buffer.append(entry)
        self._console.write(entry)
        if emit_event and self._event_emitter is not None:
            try:
                # Celery's EventReceiver injects its own ``timestamp``
                # field (Unix-time float) and does arithmetic on it, so a
                # payload carrying an ISO-string ``timestamp`` would crash
                # it. Re-key ours as ``iso_timestamp``.
                payload = entry.to_dict()
                if "timestamp" in payload:
                    payload["iso_timestamp"] = payload.pop("timestamp")
                self._event_emitter(self.LOG_EVENT_NAME, payload)
            except Exception:  # pragma: no cover — emitter is best-effort
                # Never let log-shipping break the deployment; just swallow
                # and continue. The buffered transcript still has the entry.
                pass

    def _build(
        self,
        message: str,
        /,
        *,
        level: LogLevel,
        category: LogCategory,
        max_chars: int = 5000,
        **fields: Any,
    ) -> LogEntry:
        # ``message`` is positional-only (note the ``/``) so a context
        # kwarg literally named ``message`` (e.g. a git commit message via
        # ``resource_info(..., message=...)``) lands in ``**fields`` rather
        # than colliding with the parameter.
        cleaned = clean_text(str(message))
        truncated = False
        if len(cleaned) > max_chars:
            cleaned = truncate_text(cleaned, max_lines=50, max_chars=max_chars)
            truncated = True
        # Slot keys are recognised dataclass fields. Anything else lands
        # in ``extra``.
        slot_keys = {
            "operation",
            "duration_ms",
            "tool",
            "returncode",
            "stdout",
            "stderr",
            "phase",
            "progress_pct",
            "streaming",
        }
        slots: dict[str, Any] = {k: v for k, v in fields.items() if k in slot_keys and v is not None}
        extra: dict[str, Any] = {}
        for k, v in fields.items():
            if k in slot_keys:
                continue
            # Rename a context kwarg that collides with the top-level
            # ``message`` slot so JSON consumers don't see two values
            # for the same key after the dict is flattened.
            key_out = "context_message" if k == "message" else k
            extra[key_out] = _scalar_or_str(v)
        return LogEntry(
            timestamp=_now_iso(),
            level=level,
            category=category,
            message=cleaned,
            correlation_id=self.correlation_id,
            truncated=truncated,
            extra=extra,
            **slots,
        )

    # ----- level shortcuts (kept for backwards compat) --------------------

    def debug(self, message: str, category: LogCategory = LogCategory.DEBUG, **context: Any) -> None:
        self._record(self._build(message, level=LogLevel.DEBUG, category=category, **context))

    def info(self, message: str, category: LogCategory = LogCategory.SYSTEM, **context: Any) -> None:
        self._record(self._build(message, level=LogLevel.INFO, category=category, **context))

    def success(self, message: str, category: LogCategory = LogCategory.STATUS, **context: Any) -> None:
        self._record(self._build(message, level=LogLevel.SUCCESS, category=category, **context))

    def warning(self, message: str, category: LogCategory = LogCategory.SYSTEM, **context: Any) -> None:
        self._record(self._build(message, level=LogLevel.WARNING, category=category, **context))

    def error(self, message: str, category: LogCategory = LogCategory.ERROR, **context: Any) -> None:
        self._record(self._build(message, level=LogLevel.ERROR, category=category, **context))

    # ----- structured helpers --------------------------------------------

    def phase(self, phase_name: str) -> None:
        """Mark a major deployment phase.

        Live progress is emitted separately via ``progress()`` so the
        listener can update the DB ``progress_pct`` column. ``phase()`` only
        adds a transcript marker.
        """
        self._record(
            self._build(
                f"=== {phase_name.upper()} ===",
                level=LogLevel.INFO,
                category=LogCategory.PHASE,
                phase=phase_name,
            )
        )

    def progress(
        self,
        phase_name: str,
        idx: int,
        total: int,
        message: str = "",
        phase_names: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        """Send a progress update without buffering a per-step transcript entry.

        The buffered transcript shouldn't grow by a progress marker per
        step; we still emit the Celery custom event so the listener can
        update the DB and the UI.

        ``phase_names`` is the full ordered list of phases for this task.
        When provided, the UI can render every stepper slot with its real
        label immediately. The payload is small and safe to repeat on
        every event — the listener overwrites its cached copy.

        Do NOT put a ``timestamp`` field in the payload: Celery injects one
        itself (as a Unix-time float) and would crash on an ISO string.
        Pass the ISO time as ``iso_timestamp`` if needed downstream.
        """
        pct = max(0, min(100, round((idx / max(total, 1)) * 100)))
        if self._event_emitter is not None:
            payload: dict[str, Any] = {
                "phase": phase_name,
                "phase_index": idx,
                "total_phases": total,
                "progress_pct": pct,
                "message": message,
                "correlation_id": self.correlation_id,
                "iso_timestamp": _now_iso(),
            }
            if phase_names is not None:
                # Keep as a plain list — Celery uses JSON serialisation
                # by default and tuples are coerced anyway.
                payload["phase_names"] = list(phase_names)
            with contextlib.suppress(Exception):
                self._event_emitter(self.PROGRESS_EVENT_NAME, payload)

    def operation_start(self, operation_name: str, **context: Any) -> None:
        if self.track_timing:
            with self._stack_lock:
                self._operation_stack.append((operation_name, time.time()))
        self._record(
            self._build(
                f"Starting: {operation_name}",
                level=LogLevel.INFO,
                category=LogCategory.OPERATION,
                operation=operation_name,
                **context,
            )
        )

    def operation_end(self, operation_name: str, success: bool = True, **context: Any) -> None:
        duration_ms: float | None = None
        if self.track_timing:
            with self._stack_lock:
                if self._operation_stack:
                    last_op, start_time = self._operation_stack[-1]
                    if last_op == operation_name:
                        self._operation_stack.pop()
                        duration_ms = (time.time() - start_time) * 1000
        status = "completed" if success else "failed"
        self._record(
            self._build(
                f"{status.capitalize()}: {operation_name}",
                level=LogLevel.INFO,
                category=LogCategory.OPERATION,
                operation=operation_name,
                duration_ms=duration_ms,
                **context,
            )
        )

    def command_output(self, tool_name: str, output: str, returncode: int = 0, **context: Any) -> None:
        """Buffer a captured command output block (post-completion).

        For per-line streaming use ``tool_output_line`` instead — that one
        emits each line as its own event without 5000-char framing.
        """
        level = LogLevel.ERROR if returncode != 0 else LogLevel.INFO
        category = LogCategory.ERROR if returncode != 0 else LogCategory.OUTPUT
        self._record(
            self._build(
                output,
                level=level,
                category=category,
                tool=tool_name,
                operation=tool_name,
                returncode=returncode,
                **context,
            )
        )

    def tool_output_line(self, tool_name: str, line: str) -> None:
        """One streaming line of subprocess output.

        Skips the buffered transcript truncation entirely (the whole point of
        streaming is to ship lines as they happen) and marks ``streaming=True``
        so the frontend can render it as live tail without expecting the
        usual operation framing.
        """
        cleaned = clean_text(line)
        if not cleaned:
            return
        entry = LogEntry(
            timestamp=_now_iso(),
            level=LogLevel.INFO,
            category=LogCategory.OUTPUT,
            message=cleaned,
            correlation_id=self.correlation_id,
            tool=tool_name,
            streaming=True,
        )
        self._record(entry)

    def exception(self, message: str, exception: Exception | None = None, **context: Any) -> None:
        """Log an exception with its real traceback.

        Uses ``format_exception(type, exc, exc.__traceback__)`` rather than
        ``format_exc()`` so it works outside an active ``except`` block —
        ``format_exc()`` returned ``"NoneType: None\\n"`` in that case.
        """
        ctx: dict[str, Any] = dict(context)
        if exception is not None:
            ctx["exception_type"] = type(exception).__name__
            ctx["exception_message"] = str(exception)
            tb = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
            if len(tb) > 2000:
                tb = truncate_text(tb, max_lines=30)
            ctx["stack_trace"] = tb
        else:
            ctx.setdefault("exception_type", "Unknown")
        self._record(self._build(message, level=LogLevel.ERROR, category=LogCategory.ERROR, **ctx))

    def resource_info(self, resource_type: str, resource_name: str, **details: Any) -> None:
        self._record(
            self._build(
                f"{resource_type}: {resource_name}",
                level=LogLevel.INFO,
                category=LogCategory.SYSTEM,
                resource_type=resource_type,
                resource_name=resource_name,
                **details,
            )
        )

    # ----- inspection / export -------------------------------------------

    def get_logs_dict(self) -> list[dict[str, Any]]:
        return [e.to_dict() for e in self._buffer.snapshot()]

    def get_logs_json(self, pretty: bool = True) -> str:
        return json.dumps(self.get_logs_dict(), indent=2 if pretty else None)

    def get_logs_text(self) -> str:
        return "\n".join(str(e) for e in self._buffer.snapshot())

    def get_logs_by_category(self, category: LogCategory) -> list[dict[str, Any]]:
        return [e.to_dict() for e in self._buffer.snapshot() if e.category == category]

    def get_logs_by_level(self, level: LogLevel) -> list[dict[str, Any]]:
        return [e.to_dict() for e in self._buffer.snapshot() if e.level == level]

    def get_summary(self) -> dict[str, Any]:
        entries = self._buffer.snapshot()
        by_level: dict[str, int] = {}
        by_category: dict[str, int] = {}
        for e in entries:
            by_level[e.level.value] = by_level.get(e.level.value, 0) + 1
            by_category[e.category.value] = by_category.get(e.category.value, 0) + 1
        return {
            "total_entries": len(entries),
            "by_level": by_level,
            "by_category": by_category,
            "timestamp_range": {
                "first": entries[0].timestamp if entries else None,
                "last": entries[-1].timestamp if entries else None,
            },
        }

    def clear(self) -> None:
        self._buffer.clear()
        with self._stack_lock:
            self._operation_stack.clear()


# ----------------------------------------------------------------------------
# Factory
# ----------------------------------------------------------------------------


def get_logger(name: str, *, correlation_id: str | None = None) -> StructuredLogger:
    """Return a fresh ``StructuredLogger``.

    Each deployment task gets its own instance — the per-task buffer is
    state, so we deliberately don't memoise like ``logging.getLogger`` does.
    """
    return StructuredLogger(name, correlation_id=correlation_id)
