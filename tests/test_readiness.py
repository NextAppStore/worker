"""Tests for the post-apply readiness check (serial console markers)."""

import pytest

from app.services.readiness import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_TIMEOUT_SECONDS,
    ReadinessCheck,
    parse_readiness_checks,
    scan_console,
    wait_for_readiness,
)


class _Clock:
    """Fake monotonic clock advanced by the fake sleep."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class _Console:
    """Fake OpenStackService returning a scripted console per poll."""

    def __init__(self, logs):
        self.logs = {key: list(value) for key, value in logs.items()}
        self.calls = []

    def console_log(self, instance_id):
        self.calls.append(instance_id)
        queue = self.logs[instance_id]
        return queue.pop(0) if len(queue) > 1 else queue[0]


@pytest.fixture
def clock():
    return _Clock()


def _wait(checks, console, clock, mocker):
    return wait_for_readiness(checks, console, mocker.MagicMock(), poll_interval=15, sleep=clock.sleep, clock=clock)


@pytest.mark.unit
class TestParse:
    def test_missing_output_means_no_checks(self):
        assert parse_readiness_checks(None) == []
        assert parse_readiness_checks({"user_accounts": {"value": {}}}) == []

    def test_list_form(self):
        outputs = {"readiness_checks": {"value": [{"instance_id": "a", "name": "kurs", "timeout_seconds": 1200}]}}
        assert parse_readiness_checks(outputs) == [ReadinessCheck("a", "kurs", 1200)]

    def test_map_form_uses_key_as_name(self):
        outputs = {"readiness_checks": {"value": {"kurs": {"instance_id": "a"}}}}
        assert parse_readiness_checks(outputs) == [ReadinessCheck("a", "kurs", DEFAULT_TIMEOUT_SECONDS)]

    def test_invalid_entries_are_skipped_and_timeout_clamped(self):
        outputs = {
            "readiness_checks": {
                "value": [
                    {"name": "no-id"},
                    "garbage",
                    {"instance_id": "b", "timeout_seconds": "nope"},
                    {"instance_id": "c", "timeout_seconds": 999999},
                ]
            }
        }
        assert parse_readiness_checks(outputs) == [
            ReadinessCheck("b", "b", DEFAULT_TIMEOUT_SECONDS),
            ReadinessCheck("c", "c", MAX_TIMEOUT_SECONDS),
        ]

    def test_malformed_value_means_no_checks(self):
        assert parse_readiness_checks({"readiness_checks": {"value": "x"}}) == []
        assert parse_readiness_checks({"readiness_checks": "x"}) == []


@pytest.mark.unit
class TestScanConsole:
    def test_no_marker(self):
        assert scan_console("cloud-init running\n") is None
        assert scan_console(None) is None

    def test_ready_with_prefix(self):
        assert scan_console("[  42.1] APPSTORE-READY\nlogin: ") == ("ready", None)

    def test_failed_reason(self):
        assert scan_console("APPSTORE-FAILED: starting k3s (exit 1)") == ("failed", "starting k3s (exit 1)")

    def test_last_marker_wins(self):
        assert scan_console("APPSTORE-FAILED: x\nAPPSTORE-READY\n") == ("ready", None)
        assert scan_console("APPSTORE-READY\nAPPSTORE-FAILED:\n") == ("failed", "no reason given")


@pytest.mark.unit
class TestWait:
    def test_ready_after_some_polls(self, clock, mocker):
        console = _Console({"a": [None, "booting", "APPSTORE-READY"]})
        assert _wait([ReadinessCheck("a", "kurs", 600)], console, clock, mocker) == []
        assert console.calls == ["a", "a", "a"]
        assert clock.now == 30

    def test_failed_marker_reports_reason(self, clock, mocker):
        console = _Console({"a": ["APPSTORE-FAILED: student-anna Pending"]})
        assert _wait([ReadinessCheck("a", "kurs", 600)], console, clock, mocker) == ["kurs: student-anna Pending"]

    def test_timeout(self, clock, mocker):
        console = _Console({"a": ["still booting"]})
        errors = _wait([ReadinessCheck("a", "kurs", 60)], console, clock, mocker)
        assert errors == ["kurs: no APPSTORE-READY on the serial console after 60s"]
        assert clock.now == 60

    def test_multiple_instances_independent(self, clock, mocker):
        console = _Console({"a": ["APPSTORE-READY"], "b": ["x", "APPSTORE-FAILED: boom"]})
        checks = [ReadinessCheck("a", "t1", 600), ReadinessCheck("b", "t2", 600)]
        assert _wait(checks, console, clock, mocker) == ["t2: boom"]
        # "a" is not polled again once it reported.
        assert console.calls == ["a", "b", "b"]

    def test_no_checks_returns_immediately(self, clock, mocker):
        assert _wait([], _Console({}), clock, mocker) == []
        assert clock.now == 0
