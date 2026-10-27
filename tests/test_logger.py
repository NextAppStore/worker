"""Tests for app.utils.logger.

The instructions mention helpers named ``clean_log_line``, ``is_verbose_line``,
``filter_logs``, ``format_logs`` and ``_get_icon``; none of those are exported
by ``app.utils.logger``. Per the task brief ("Skip any helper that is not
actually exported by the module — do not invent functions"), these tests
cover the helpers that DO exist (``clean_text``, ``truncate_text``,
``_scalar_or_str``, ``_now_iso``, ``LogEntry``, ``StructuredLogger``,
``get_logger``) and include a smoke test verifying that ``get_logger``
returns a logger whose common methods (``info``, ``error``, ``success``,
``exception``, ``operation_start``, ``operation_end``, ``command_output``,
``debug``) are callable without raising.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest

from app.utils import logger as logger_mod
from app.utils.logger import (
    LogCategory,
    LogEntry,
    LogLevel,
    StructuredLogger,
    _scalar_or_str,
    clean_text,
    get_logger,
    truncate_text,
)

# ---------------------------------------------------------------------------
# clean_text
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCleanText:
    def test_strips_ansi_color_escape(self):
        """clean_text removes ANSI color codes."""
        assert clean_text("\x1b[31mred\x1b[0m") == "red"

    def test_strips_leading_and_trailing_whitespace(self):
        """clean_text trims surrounding whitespace."""
        assert clean_text("   hello world   ") == "hello world"

    def test_handles_plain_text_unchanged(self):
        """clean_text returns plain text trimmed but otherwise identical."""
        assert clean_text("nothing fancy here") == "nothing fancy here"

    def test_returns_empty_string_for_only_whitespace(self):
        """clean_text returns empty string when input is only whitespace."""
        assert clean_text("   \n\t  ") == ""

    def test_strips_multiple_ansi_sequences(self):
        """clean_text removes multiple ANSI escape sequences in a single string."""
        text = "\x1b[1m\x1b[33mwarn\x1b[0m: \x1b[31mfail\x1b[0m"
        assert clean_text(text) == "warn: fail"


# ---------------------------------------------------------------------------
# truncate_text
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTruncateText:
    def test_returns_unchanged_when_within_limits(self):
        """truncate_text returns text untouched when under both limits."""
        text = "line1\nline2\nline3"
        assert truncate_text(text, max_lines=50, max_chars=5000) == text

    def test_truncates_when_too_many_lines(self):
        """truncate_text keeps head and tail when line count exceeds max_lines."""
        lines = [f"line{i}" for i in range(200)]
        text = "\n".join(lines)
        result = truncate_text(text, max_lines=50, max_chars=100000)
        assert "line0" in result
        assert "line199" in result  # tail preserved
        assert "truncated" in result
        # head is 15 lines; tail is max_lines - 16 = 34 lines; +1 marker = 50
        assert result.count("\n") == 49

    def test_truncates_when_text_too_long_keeps_tail(self):
        """truncate_text keeps the tail when text exceeds max_chars."""
        text = "A" * 100 + "B" * 1000 + "ZZZTAILZZZ"
        result = truncate_text(text, max_lines=500, max_chars=200)
        assert "ZZZTAILZZZ" in result
        assert "truncated" in result

    def test_truncates_both_lines_and_chars(self):
        """truncate_text applies both line and char limits when both exceeded."""
        # Many lines, each big enough to overflow char budget too.
        lines = [f"line{i}-" + "x" * 100 for i in range(200)]
        text = "\n".join(lines)
        result = truncate_text(text, max_lines=50, max_chars=300)
        # The char cap is the dominant truncation; both messages will appear
        # because the line cap fires first then the char cap on the joined text.
        assert "truncated" in result
        assert len(result) < len(text)


# ---------------------------------------------------------------------------
# _scalar_or_str
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestScalarOrStr:
    def test_passes_through_string(self):
        """_scalar_or_str returns strings unchanged."""
        assert _scalar_or_str("hello") == "hello"

    def test_passes_through_int(self):
        """_scalar_or_str returns ints unchanged."""
        assert _scalar_or_str(42) == 42

    def test_passes_through_float(self):
        """_scalar_or_str returns floats unchanged."""
        assert _scalar_or_str(3.14) == 3.14

    def test_passes_through_bool(self):
        """_scalar_or_str returns bools unchanged."""
        assert _scalar_or_str(True) is True

    def test_passes_through_none(self):
        """_scalar_or_str returns None unchanged."""
        assert _scalar_or_str(None) is None

    def test_recurses_into_dict(self):
        """_scalar_or_str recurses into dicts, converting non-scalar leaves."""

        class Custom:
            def __str__(self) -> str:
                return "custom-repr"

        result = _scalar_or_str({"a": 1, "b": Custom()})
        assert result == {"a": 1, "b": "custom-repr"}

    def test_recurses_into_list(self):
        """_scalar_or_str recurses into lists."""

        class Custom:
            def __str__(self) -> str:
                return "X"

        assert _scalar_or_str([1, "two", Custom()]) == [1, "two", "X"]

    def test_recurses_into_tuple_returns_list(self):
        """_scalar_or_str converts tuples to lists with recursion."""
        assert _scalar_or_str((1, 2, 3)) == [1, 2, 3]

    def test_falls_back_to_str_for_unknown(self):
        """_scalar_or_str stringifies unknown types."""

        class Unknown:
            def __str__(self) -> str:
                return "stringified"

        assert _scalar_or_str(Unknown()) == "stringified"


# ---------------------------------------------------------------------------
# _now_iso
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNowIso:
    def test_returns_iso_string_with_z_suffix(self):
        """_now_iso returns an ISO-8601 string ending in 'Z'."""
        result = logger_mod._now_iso()
        assert isinstance(result, str)
        assert result.endswith("Z")
        assert "+00:00" not in result

    def test_contains_date_separator(self):
        """_now_iso contains 'T' separating date and time."""
        assert "T" in logger_mod._now_iso()


# ---------------------------------------------------------------------------
# LogEntry
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLogEntry:
    def _make(self, **overrides: Any) -> LogEntry:
        base: dict[str, Any] = {
            "timestamp": "2026-01-01T00:00:00Z",
            "level": LogLevel.INFO,
            "category": LogCategory.SYSTEM,
            "message": "hello",
        }
        base.update(overrides)
        return LogEntry(**base)

    def test_to_dict_skips_none_optional_fields(self):
        """LogEntry.to_dict omits optional fields that are None."""
        d = self._make().to_dict()
        assert d["timestamp"] == "2026-01-01T00:00:00Z"
        assert d["level"] == "INFO"
        assert d["category"] == "system"
        assert d["message"] == "hello"
        # None-valued slots should not appear:
        for key in ("correlation_id", "operation", "duration_ms", "tool"):
            assert key not in d

    def test_to_dict_rounds_duration_ms(self):
        """LogEntry.to_dict rounds duration_ms to 2 decimal places."""
        d = self._make(duration_ms=123.4567).to_dict()
        assert d["duration_ms"] == 123.46

    def test_to_dict_includes_streaming_flag_when_true(self):
        """LogEntry.to_dict includes streaming=True only when set."""
        on = self._make(streaming=True).to_dict()
        off = self._make().to_dict()
        assert on["streaming"] is True
        assert "streaming" not in off

    def test_to_dict_includes_truncated_flag_when_true(self):
        """LogEntry.to_dict includes truncated=True only when set."""
        on = self._make(truncated=True).to_dict()
        off = self._make().to_dict()
        assert on["truncated"] is True
        assert "truncated" not in off

    def test_to_dict_flattens_extra_without_overwriting(self):
        """LogEntry.to_dict merges extra into top-level but never overwrites."""
        d = self._make(extra={"foo": "bar", "message": "ignored"}).to_dict()
        assert d["foo"] == "bar"
        # Existing 'message' must not be overwritten by extra
        assert d["message"] == "hello"

    def test_str_renders_icon_and_timestamp(self):
        """LogEntry.__str__ contains the level icon, timestamp and message."""
        s = str(self._make(level=LogLevel.SUCCESS))
        assert "✓" in s
        assert "2026-01-01T00:00:00Z" in s
        assert "hello" in s

    def test_str_includes_duration_when_present(self):
        """LogEntry.__str__ appends duration in ms when duration_ms is set."""
        s = str(self._make(duration_ms=12.5))
        assert "[12.50ms]" in s

    def test_str_uses_fallback_icon_for_unknown_level(self, mocker):
        """LogEntry.__str__ falls back to '•' for levels missing from the icon map."""
        mocker.patch.dict(logger_mod._LEVEL_ICONS, {}, clear=True)
        s = str(self._make(level=LogLevel.INFO))
        assert "•" in s


# ---------------------------------------------------------------------------
# StructuredLogger / get_logger
# ---------------------------------------------------------------------------


@pytest.fixture
def slogger() -> StructuredLogger:
    """A StructuredLogger with the console sink disabled."""
    return StructuredLogger("test.logger", correlation_id="corr-1", console=False)


@pytest.mark.unit
class TestStructuredLoggerLevels:
    def test_info_records_an_entry(self, slogger):
        """info() appends one entry with level INFO."""
        slogger.info("hello")
        entries = slogger.get_logs_dict()
        assert len(entries) == 1
        assert entries[0]["level"] == "INFO"
        assert entries[0]["message"] == "hello"
        assert entries[0]["correlation_id"] == "corr-1"

    def test_debug_records_debug_level(self, slogger):
        """debug() appends an entry with level DEBUG and category debug."""
        slogger.debug("dbg")
        e = slogger.get_logs_dict()[0]
        assert e["level"] == "DEBUG"
        assert e["category"] == "debug"

    def test_success_records_success_level(self, slogger):
        """success() appends an entry with level SUCCESS."""
        slogger.success("done")
        assert slogger.get_logs_dict()[0]["level"] == "SUCCESS"

    def test_warning_records_warning_level(self, slogger):
        """warning() appends an entry with level WARNING."""
        slogger.warning("careful")
        assert slogger.get_logs_dict()[0]["level"] == "WARNING"

    def test_error_records_error_level(self, slogger):
        """error() appends an entry with level ERROR."""
        slogger.error("boom")
        assert slogger.get_logs_dict()[0]["level"] == "ERROR"

    def test_message_is_cleaned_of_ansi(self, slogger):
        """Messages have ANSI sequences removed before being recorded."""
        slogger.info("\x1b[31mred\x1b[0m message")
        assert slogger.get_logs_dict()[0]["message"] == "red message"


@pytest.mark.unit
class TestStructuredLoggerBuild:
    def test_long_message_is_truncated_and_flag_set(self, slogger):
        """Building an entry whose message exceeds max_chars sets truncated=True."""
        slogger.info("x" * 6000)
        e = slogger.get_logs_dict()[0]
        assert e.get("truncated") is True
        assert len(e["message"]) < 6000

    def test_context_kwarg_named_message_goes_into_extra_renamed(self, slogger):
        """A context kwarg named 'message' is renamed to 'context_message' in extra.

        resource_info forwards arbitrary **details into _build, exercising the
        positional-only message guard against a colliding 'message' kwarg.
        """
        slogger.resource_info("commit", "abc123", message="context-value")
        e = slogger.get_logs_dict()[0]
        assert e["message"] == "commit: abc123"
        assert e["context_message"] == "context-value"

    def test_known_slot_kwargs_land_on_entry(self, slogger):
        """Slot-named context kwargs are stored on the dataclass field, not in extra."""
        slogger.info("msg", tool="terraform", returncode=2, phase="init")
        e = slogger.get_logs_dict()[0]
        assert e["tool"] == "terraform"
        assert e["returncode"] == 2
        assert e["phase"] == "init"

    def test_unknown_context_lands_in_extra_flattened(self, slogger):
        """Unknown context kwargs are coerced and merged into the top level."""

        class Custom:
            def __str__(self) -> str:
                return "custom!"

        slogger.info("msg", some_key=Custom(), another=42)
        e = slogger.get_logs_dict()[0]
        assert e["some_key"] == "custom!"
        assert e["another"] == 42


@pytest.mark.unit
class TestStructuredLoggerPhase:
    def test_phase_records_uppercase_marker(self, slogger):
        """phase() emits an entry whose message is the phase name in uppercase."""
        slogger.phase("packer_build")
        e = slogger.get_logs_dict()[0]
        assert e["message"] == "=== PACKER_BUILD ==="
        assert e["category"] == "phase"
        assert e["phase"] == "packer_build"


@pytest.mark.unit
class TestStructuredLoggerProgress:
    def test_progress_does_not_buffer_entries(self, slogger):
        """progress() must not append anything to the buffered transcript."""
        slogger.progress("phase1", 1, 5)
        assert slogger.get_logs_dict() == []

    def test_progress_emits_event_with_clamped_pct(self):
        """progress() emits a PROGRESS event with progress_pct clamped to 0..100."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("p", console=False, event_emitter=lambda n, p: events.append((n, p)))
        log.progress("phase1", 2, 4, "halfway")
        assert len(events) == 1
        name, payload = events[0]
        assert name == StructuredLogger.PROGRESS_EVENT_NAME
        assert payload["progress_pct"] == 50
        assert payload["message"] == "halfway"
        assert payload["phase"] == "phase1"
        assert "timestamp" not in payload  # the frontend reads iso_timestamp
        assert "iso_timestamp" in payload

    def test_progress_clamps_over_100(self):
        """progress() clamps progress_pct to 100 when idx > total."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("p", console=False, event_emitter=lambda n, p: events.append((n, p)))
        log.progress("phase1", 10, 5)
        assert events[0][1]["progress_pct"] == 100

    def test_progress_with_zero_total_does_not_divide_by_zero(self):
        """progress() handles total=0 without raising."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("p", console=False, event_emitter=lambda n, p: events.append((n, p)))
        log.progress("phase1", 0, 0)
        assert events  # an event was emitted
        assert 0 <= events[0][1]["progress_pct"] <= 100

    def test_progress_includes_phase_names_when_given(self):
        """progress() forwards phase_names as a list."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("p", console=False, event_emitter=lambda n, p: events.append((n, p)))
        log.progress("p1", 1, 2, phase_names=("p1", "p2"))
        assert events[0][1]["phase_names"] == ["p1", "p2"]

    def test_progress_without_emitter_is_a_noop(self, slogger):
        """progress() is a noop when no event_emitter is configured."""
        slogger.progress("phase", 1, 3)  # must not raise
        assert slogger.get_logs_dict() == []

    def test_progress_suppresses_emitter_exceptions(self):
        """progress() must swallow exceptions raised by the emitter."""

        def bad(_name, _payload):
            raise RuntimeError("nope")

        log = StructuredLogger("p", console=False, event_emitter=bad)
        log.progress("phase", 1, 2)  # must not raise


@pytest.mark.unit
class TestStructuredLoggerOperations:
    def test_operation_start_pushes_stack_and_records(self, slogger):
        """operation_start records an entry and pushes onto the timing stack."""
        slogger.operation_start("plan")
        e = slogger.get_logs_dict()[0]
        assert e["operation"] == "plan"
        assert e["message"].startswith("Starting:")

    def test_operation_end_pops_and_records_duration(self, slogger):
        """operation_end completes a matched op and records a duration_ms."""
        slogger.operation_start("plan")
        slogger.operation_end("plan", success=True)
        end_entry = slogger.get_logs_dict()[-1]
        assert end_entry["operation"] == "plan"
        assert end_entry["message"].startswith("Completed:")
        assert end_entry.get("duration_ms") is not None

    def test_operation_end_failure_uses_failed_capitalization(self, slogger):
        """operation_end(success=False) prefixes the message with 'Failed:'."""
        slogger.operation_start("apply")
        slogger.operation_end("apply", success=False)
        msg = slogger.get_logs_dict()[-1]["message"]
        assert msg.startswith("Failed:")

    def test_operation_end_without_matching_start_records_no_duration(self, slogger):
        """operation_end with no matching start records an entry but no duration."""
        slogger.operation_end("never-started", success=True)
        e = slogger.get_logs_dict()[-1]
        assert "duration_ms" not in e

    def test_operation_end_with_mismatched_top_does_not_pop(self, slogger):
        """operation_end whose name does not match the stack top leaves the stack alone."""
        slogger.operation_start("outer")
        slogger.operation_end("other-op", success=True)
        # No duration recorded for 'other-op'
        e = slogger.get_logs_dict()[-1]
        assert "duration_ms" not in e

    def test_track_timing_disabled_skips_stack(self):
        """track_timing=False means operation_end records no duration."""
        log = StructuredLogger("t", console=False, track_timing=False)
        log.operation_start("op")
        log.operation_end("op", success=True)
        end_entry = log.get_logs_dict()[-1]
        assert "duration_ms" not in end_entry


@pytest.mark.unit
class TestStructuredLoggerCommandOutput:
    def test_command_output_success_uses_info_level(self, slogger):
        """command_output with returncode=0 uses INFO/OUTPUT."""
        slogger.command_output("packer", "all good", returncode=0)
        e = slogger.get_logs_dict()[-1]
        assert e["level"] == "INFO"
        assert e["category"] == "output"
        assert e["tool"] == "packer"
        assert e["returncode"] == 0

    def test_command_output_failure_uses_error_level(self, slogger):
        """command_output with non-zero returncode uses ERROR/ERROR."""
        slogger.command_output("packer", "boom", returncode=1)
        e = slogger.get_logs_dict()[-1]
        assert e["level"] == "ERROR"
        assert e["category"] == "error"
        assert e["returncode"] == 1


@pytest.mark.unit
class TestStructuredLoggerToolOutputLine:
    def test_tool_output_line_skipped_when_blank(self, slogger):
        """tool_output_line drops empty lines (post-clean)."""
        slogger.tool_output_line("packer", "   ")
        assert slogger.get_logs_dict() == []

    def test_tool_output_line_records_streaming_entry(self, slogger):
        """tool_output_line emits a streaming entry with the tool name."""
        slogger.tool_output_line("terraform", "applying...")
        e = slogger.get_logs_dict()[0]
        assert e["streaming"] is True
        assert e["tool"] == "terraform"
        assert e["message"] == "applying..."


@pytest.mark.unit
class TestStructuredLoggerException:
    def test_exception_with_real_exception_includes_traceback(self, slogger):
        """exception() with a real Exception adds stack_trace/type/message context."""
        try:
            raise ValueError("explode")
        except ValueError as exc:
            slogger.exception("failed", exception=exc)
        e = slogger.get_logs_dict()[-1]
        assert e["exception_type"] == "ValueError"
        assert e["exception_message"] == "explode"
        assert "stack_trace" in e
        assert "ValueError" in e["stack_trace"]

    def test_exception_truncates_long_traceback(self, slogger, mocker):
        """exception() truncates tracebacks longer than 2000 chars."""
        # truncate_text triggers on max_lines OR max_chars; the production
        # call passes max_lines=30 only, so we need many lines (default
        # max_chars=5000 won't fire on a one-line giant). Build 50 lines.
        big_lines = [f"frame {i}: some content here" for i in range(50)]
        mocker.patch(
            "app.utils.logger.traceback.format_exception",
            return_value=["\n".join(big_lines) + "\n"],
        )
        # Inflate the joined text past 2000 chars by padding each frame.
        big_lines = [f"frame {i}: " + "X" * 60 for i in range(60)]
        mocker.patch(
            "app.utils.logger.traceback.format_exception",
            return_value=["\n".join(big_lines)],
        )
        slogger.exception("boom", exception=RuntimeError("x"))
        e = slogger.get_logs_dict()[-1]
        assert "truncated" in e["stack_trace"]

    def test_exception_without_exception_argument_uses_unknown_type(self, slogger):
        """exception() with exception=None records exception_type='Unknown'."""
        slogger.exception("something broke")
        e = slogger.get_logs_dict()[-1]
        assert e["exception_type"] == "Unknown"


@pytest.mark.unit
class TestStructuredLoggerResourceInfo:
    def test_resource_info_records_resource_fields(self, slogger):
        """resource_info builds a message containing the resource type and name."""
        slogger.resource_info("vm", "my-vm", flavor="m1.small")
        e = slogger.get_logs_dict()[-1]
        assert e["message"] == "vm: my-vm"
        assert e["resource_type"] == "vm"
        assert e["resource_name"] == "my-vm"
        assert e["flavor"] == "m1.small"


@pytest.mark.unit
class TestStructuredLoggerExportAndInspect:
    def test_get_logs_json_pretty_is_valid_json(self, slogger):
        """get_logs_json(pretty=True) returns parsable indented JSON."""
        slogger.info("a")
        text = slogger.get_logs_json(pretty=True)
        loaded = json.loads(text)
        assert isinstance(loaded, list)
        assert loaded[0]["message"] == "a"

    def test_get_logs_json_non_pretty(self, slogger):
        """get_logs_json(pretty=False) still produces valid JSON."""
        slogger.info("a")
        text = slogger.get_logs_json(pretty=False)
        loaded = json.loads(text)
        assert loaded[0]["message"] == "a"

    def test_get_logs_text_joins_lines(self, slogger):
        """get_logs_text returns one line per entry."""
        slogger.info("first")
        slogger.info("second")
        text = slogger.get_logs_text()
        assert "first" in text and "second" in text
        assert text.count("\n") == 1

    def test_get_logs_by_category_filters(self, slogger):
        """get_logs_by_category returns only the matching category."""
        slogger.info("sys-msg", category=LogCategory.SYSTEM)
        slogger.info("op-msg", category=LogCategory.OPERATION)
        result = slogger.get_logs_by_category(LogCategory.OPERATION)
        assert len(result) == 1
        assert result[0]["message"] == "op-msg"

    def test_get_logs_by_level_filters(self, slogger):
        """get_logs_by_level returns only entries with that level."""
        slogger.info("hi")
        slogger.error("bye")
        errors = slogger.get_logs_by_level(LogLevel.ERROR)
        assert len(errors) == 1
        assert errors[0]["message"] == "bye"

    def test_get_summary_counts_by_level_and_category(self, slogger):
        """get_summary returns counts grouped by level/category and timestamp range."""
        slogger.info("a")
        slogger.error("b")
        summary = slogger.get_summary()
        assert summary["total_entries"] == 2
        assert summary["by_level"]["INFO"] == 1
        assert summary["by_level"]["ERROR"] == 1
        assert summary["by_category"]["system"] == 1
        assert summary["by_category"]["error"] == 1
        assert summary["timestamp_range"]["first"] is not None
        assert summary["timestamp_range"]["last"] is not None

    def test_get_summary_empty_buffer_has_null_range(self, slogger):
        """get_summary on an empty buffer reports None for the timestamp range."""
        summary = slogger.get_summary()
        assert summary["total_entries"] == 0
        assert summary["timestamp_range"]["first"] is None
        assert summary["timestamp_range"]["last"] is None

    def test_clear_empties_buffer_and_stack(self, slogger):
        """clear() removes buffered entries and pending operations."""
        slogger.operation_start("op")
        slogger.info("msg")
        slogger.clear()
        assert slogger.get_logs_dict() == []
        # subsequent end without start should NOT have a duration
        slogger.operation_end("op", success=True)
        e = slogger.get_logs_dict()[-1]
        assert "duration_ms" not in e


@pytest.mark.unit
class TestStructuredLoggerEventEmitter:
    def test_event_emitter_called_with_renamed_timestamp(self):
        """_record renames 'timestamp' to 'iso_timestamp' for log events."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("t", console=False, event_emitter=lambda n, p: events.append((n, p)))
        log.info("hello")
        assert events
        name, payload = events[0]
        assert name == StructuredLogger.LOG_EVENT_NAME
        assert "timestamp" not in payload
        assert "iso_timestamp" in payload

    def test_event_emitter_exception_is_swallowed(self):
        """_record swallows emitter errors and still buffers the entry."""

        def bad(_name, _payload):
            raise RuntimeError("kaboom")

        log = StructuredLogger("t", console=False, event_emitter=bad)
        log.info("still recorded")  # must not raise
        assert log.get_logs_dict()[0]["message"] == "still recorded"

    def test_set_event_emitter_attaches_and_detaches(self):
        """set_event_emitter swaps the emitter at runtime."""
        events: list[tuple[str, dict[str, Any]]] = []
        log = StructuredLogger("t", console=False)
        log.info("before")
        assert events == []  # no emitter
        log.set_event_emitter(lambda n, p: events.append((n, p)))
        log.info("after")
        assert len(events) == 1
        log.set_event_emitter(None)
        log.info("detached")
        assert len(events) == 1


@pytest.mark.unit
class TestConsoleSink:
    def test_console_sink_writes_to_named_logger(self, mocker, caplog):
        """When console=True, entries are forwarded to the stdlib logger."""
        mocker.patch.dict("os.environ", {"WORKER_LOG_CONSOLE": "1"})
        log = StructuredLogger("sink.test", console=True)
        with caplog.at_level(logging.DEBUG, logger="sink.test"):
            log.info("forwarded")
        assert any("forwarded" in r.message for r in caplog.records)

    def test_console_sink_disabled_via_env(self, mocker, caplog):
        """Setting WORKER_LOG_CONSOLE=0 disables the stdlib forwarder."""
        mocker.patch.dict("os.environ", {"WORKER_LOG_CONSOLE": "0"})
        log = StructuredLogger("sink.disabled", console=True)
        with caplog.at_level(logging.DEBUG, logger="sink.disabled"):
            log.info("not forwarded")
        assert not any("not forwarded" in r.message for r in caplog.records)

    def test_console_sink_disabled_via_constructor_arg(self, mocker, caplog):
        """console=False disables the sink even when the env var allows it."""
        mocker.patch.dict("os.environ", {"WORKER_LOG_CONSOLE": "1"})
        log = StructuredLogger("sink.ctor", console=False)
        with caplog.at_level(logging.DEBUG, logger="sink.ctor"):
            log.info("blocked")
        assert not any("blocked" in r.message for r in caplog.records)

    def test_console_sink_error_level_routes_to_logger_error(self, mocker, caplog):
        """ERROR-level entries are routed to logger.error."""
        mocker.patch.dict("os.environ", {"WORKER_LOG_CONSOLE": "1"})
        log = StructuredLogger("sink.err", console=True)
        with caplog.at_level(logging.DEBUG, logger="sink.err"):
            log.error("boom-err")
        matching = [r for r in caplog.records if "boom-err" in r.message]
        assert matching
        assert matching[0].levelno == logging.ERROR


# ---------------------------------------------------------------------------
# get_logger smoke test
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetLoggerSmoke:
    def test_get_logger_returns_structured_logger(self):
        """get_logger returns a StructuredLogger instance with correlation id."""
        log = get_logger("smoke", correlation_id="abc")
        assert isinstance(log, StructuredLogger)
        assert log.correlation_id == "abc"

    def test_get_logger_returns_fresh_instance_each_call(self):
        """Each get_logger call returns a new buffer (not memoised)."""
        a = get_logger("smoke-a")
        b = get_logger("smoke-a")
        a.info("only on a")
        assert a.get_logs_dict() != b.get_logs_dict()

    def test_common_methods_callable_without_raising(self, mocker):
        """get_logger().{info,error,success,exception,operation_start/end,command_output,debug} all callable."""
        mocker.patch.dict("os.environ", {"WORKER_LOG_CONSOLE": "0"})
        log = get_logger("smoke.methods")
        log.info("i")
        log.error("e")
        log.success("s")
        log.exception("ex", exception=ValueError("v"))
        log.operation_start("op")
        log.operation_end("op", success=True)
        log.command_output("tool", "out", returncode=0)
        log.debug("d")
        # All entries should have made it into the buffer.
        assert len(log.get_logs_dict()) >= 8
