"""Tests for the PackerExecutor service."""

import json
import subprocess

import pytest

from app.services.packer_executor import PackerExecutor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class StreamRecorder:
    """Callable recorder replacing ``_stream_subprocess``.

    Captures invocation kwargs and returns a configurable
    ``(returncode, stdout, stderr)`` triple.
    """

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "", raise_exc: Exception | None = None):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.raise_exc = raise_exc
        self.calls: list[dict] = []

    def __call__(self, cmd, cwd, env, timeout, tool_name, output_callback=None):
        self.calls.append(
            {
                "cmd": cmd,
                "cwd": cwd,
                "env": env,
                "timeout": timeout,
                "tool_name": tool_name,
                "output_callback": output_callback,
            }
        )
        if self.raise_exc:
            raise self.raise_exc
        return self.returncode, self.stdout, self.stderr


@pytest.fixture
def working_dir(tmp_path):
    """Working directory for Packer commands."""
    return str(tmp_path)


@pytest.fixture
def executor(working_dir):
    """Default PackerExecutor instance for tests."""
    return PackerExecutor(working_dir=working_dir)


# ---------------------------------------------------------------------------
# init()
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPackerInit:
    """Behaviour of ``PackerExecutor.init``."""

    def test_init_success_returns_true_with_stdout_stderr(self, mocker, executor, working_dir):
        """init returns (True, stdout, stderr) when subprocess exits 0."""
        recorder = StreamRecorder(returncode=0, stdout="installed", stderr="warn")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout, stderr = executor.init()

        assert success is True
        assert stdout == "installed"
        assert stderr == "warn"

    def test_init_failure_returns_false_with_streams(self, mocker, executor):
        """init returns (False, stdout, stderr) on non-zero returncode."""
        recorder = StreamRecorder(returncode=1, stdout="some out", stderr="some err")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout, stderr = executor.init()

        assert success is False
        assert stdout == "some out"
        assert stderr == "some err"

    def test_init_timeout_returncode_124_branch(self, mocker, executor):
        """init treats returncode 124 as failure (timeout branch)."""
        recorder = StreamRecorder(returncode=124, stdout="", stderr="")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, _, _ = executor.init()

        assert success is False

    def test_init_command_shape_is_packer_init_dot(self, mocker, executor, working_dir):
        """init invokes [packer_path, init, .] at the working_dir."""
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        executor.init()

        call = recorder.calls[0]
        assert call["cmd"][-2:] == ["init", "."]
        assert call["cmd"][0] == executor.packer_path
        assert call["cwd"] == working_dir
        assert call["tool_name"] == "packer_init"
        assert call["timeout"] == 300

    def test_init_env_has_packer_log_enabled(self, mocker, executor):
        """init exports PACKER_LOG=1 in the subprocess environment."""
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        executor.init()

        assert recorder.calls[0]["env"]["PACKER_LOG"] == "1"

    def test_init_forwards_output_callback(self, mocker, working_dir):
        """init forwards the executor's output_callback to _stream_subprocess."""
        cb = lambda line: None  # noqa: E731
        ex = PackerExecutor(working_dir=working_dir, output_callback=cb)
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        ex.init()

        assert recorder.calls[0]["output_callback"] is cb

    def test_init_exception_returns_false_empty_stdout_message(self, mocker, executor):
        """init catches exceptions and returns (False, '', str(e))."""
        recorder = StreamRecorder(raise_exc=RuntimeError("boom"))
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout, stderr = executor.init()

        assert success is False
        assert stdout == ""
        assert stderr == "boom"


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPackerValidate:
    """Behaviour of ``PackerExecutor.validate``."""

    def _make_completed(self, returncode=0, stdout="ok", stderr=""):
        return subprocess.CompletedProcess(args=["packer"], returncode=returncode, stdout=stdout, stderr=stderr)

    def test_validate_no_variables_command_shape(self, mocker, executor, working_dir):
        """validate with no variables runs [packer, validate, '.']."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())

        success, stdout, stderr = executor.validate("template.pkr.hcl")

        assert success is True
        assert stdout == "ok"
        assert stderr == ""
        cmd = run.call_args.kwargs["cmd"] if "cmd" in run.call_args.kwargs else run.call_args.args[0]
        assert cmd == [executor.packer_path, "validate", "."]
        assert run.call_args.kwargs["cwd"] == working_dir
        assert run.call_args.kwargs["timeout"] == 60
        assert run.call_args.kwargs["capture_output"] is True
        assert run.call_args.kwargs["text"] is True

    def test_validate_encodes_dict_variable_as_json(self, mocker, executor):
        """dict variable values are JSON-encoded via json.dumps."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())
        variables = {"mapping": {"a": 1, "b": 2}}

        executor.validate("template.pkr.hcl", variables=variables)

        cmd = run.call_args.args[0]
        assert "-var" in cmd
        idx = cmd.index("-var")
        assert cmd[idx + 1] == f"mapping={json.dumps({'a': 1, 'b': 2})}"

    def test_validate_encodes_list_variable_as_json(self, mocker, executor):
        """list variable values are JSON-encoded via json.dumps."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())
        variables = {"items": [1, 2, 3]}

        executor.validate("template.pkr.hcl", variables=variables)

        cmd = run.call_args.args[0]
        assert f"items={json.dumps([1, 2, 3])}" in cmd

    def test_validate_encodes_primitive_variable_via_str(self, mocker, executor):
        """primitive variable values are stringified via str()."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())
        variables = {"name": "foo", "count": 5, "enabled": True}

        executor.validate("template.pkr.hcl", variables=variables)

        cmd = run.call_args.args[0]
        assert "name=foo" in cmd
        assert "count=5" in cmd
        assert "enabled=True" in cmd

    def test_validate_dot_appended_after_vars(self, mocker, executor):
        """validate appends '.' as the final command element after vars."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())

        executor.validate("template.pkr.hcl", variables={"x": "y"})

        cmd = run.call_args.args[0]
        assert cmd[-1] == "."

    def test_validate_success_returncode_zero(self, mocker, executor):
        """validate returns (True, stdout, stderr) on rc=0."""
        mocker.patch(
            "app.services.packer_executor.subprocess.run",
            return_value=self._make_completed(returncode=0, stdout="good", stderr=""),
        )

        success, stdout, stderr = executor.validate("template.pkr.hcl")

        assert success is True
        assert stdout == "good"

    def test_validate_failure_nonzero_returncode(self, mocker, executor):
        """validate returns (False, stdout, stderr) when rc != 0."""
        mocker.patch(
            "app.services.packer_executor.subprocess.run",
            return_value=self._make_completed(returncode=2, stdout="", stderr="Error: bad template"),
        )

        success, stdout, stderr = executor.validate("template.pkr.hcl")

        assert success is False
        assert stderr == "Error: bad template"

    def test_validate_exception_returns_false_empty_stdout_message(self, mocker, executor):
        """validate catches exceptions and returns (False, '', str(e))."""
        mocker.patch(
            "app.services.packer_executor.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="packer", timeout=60),
        )

        success, stdout, stderr = executor.validate("template.pkr.hcl")

        assert success is False
        assert stdout == ""
        assert stderr  # str(exception) is non-empty

    def test_validate_env_has_packer_log_enabled(self, mocker, executor):
        """validate's subprocess env contains PACKER_LOG=1."""
        run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=self._make_completed())

        executor.validate("template.pkr.hcl")

        assert run.call_args.kwargs["env"]["PACKER_LOG"] == "1"


# ---------------------------------------------------------------------------
# build()
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPackerBuild:
    """Behaviour of ``PackerExecutor.build``."""

    def test_build_force_true_adds_force_flag(self, mocker, executor):
        """build with force=True adds the -force flag."""
        recorder = StreamRecorder(returncode=0, stdout="built\n")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        executor.build("template.pkr.hcl", force=True)

        cmd = recorder.calls[0]["cmd"]
        assert "-force" in cmd
        assert cmd[0] == executor.packer_path
        assert cmd[1] == "build"

    def test_build_force_false_omits_force_flag(self, mocker, executor):
        """build with force=False does not include the -force flag."""
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        executor.build("template.pkr.hcl", force=False)

        assert "-force" not in recorder.calls[0]["cmd"]

    def test_build_encodes_variables_identically_to_validate(self, mocker, executor):
        """build encodes dict/list via JSON and primitives via str()."""
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)
        variables = {
            "mapping": {"a": 1},
            "items": [1, 2],
            "name": "foo",
            "count": 7,
        }

        executor.build("template.pkr.hcl", variables=variables, force=False)

        cmd = recorder.calls[0]["cmd"]
        assert f"mapping={json.dumps({'a': 1})}" in cmd
        assert f"items={json.dumps([1, 2])}" in cmd
        assert "name=foo" in cmd
        assert "count=7" in cmd
        # final element is the working dir marker
        assert cmd[-1] == "."

    def test_build_timeout_is_one_hour(self, mocker, executor):
        """build invokes _stream_subprocess with timeout=3600."""
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        executor.build("template.pkr.hcl")

        assert recorder.calls[0]["timeout"] == 3600
        assert recorder.calls[0]["tool_name"] == "packer_build"

    def test_build_success_returncode_zero_returns_true(self, mocker, executor):
        """rc=0 yields (True, stdout)."""
        recorder = StreamRecorder(returncode=0, stdout="line1\nline2")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout = executor.build("template.pkr.hcl")

        assert success is True
        assert stdout == "line1\nline2"

    def test_build_failure_returncode_one_returns_false(self, mocker, executor):
        """rc=1 yields (False, stdout) and triggers the generic failure branch."""
        recorder = StreamRecorder(returncode=1, stdout="oops")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout = executor.build("template.pkr.hcl")

        assert success is False
        assert stdout == "oops"

    def test_build_timeout_returncode_124(self, mocker, executor):
        """rc=124 takes the timeout-specific failure branch."""
        recorder = StreamRecorder(returncode=124, stdout="")
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, _ = executor.build("template.pkr.hcl")

        assert success is False

    def test_build_forwards_output_callback(self, mocker, working_dir):
        """build forwards output_callback to _stream_subprocess."""
        sink: list[str] = []
        cb = sink.append
        ex = PackerExecutor(working_dir=working_dir, output_callback=cb)
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        ex.build("template.pkr.hcl")

        assert recorder.calls[0]["output_callback"] is cb

    def test_build_merges_extra_env(self, mocker, working_dir):
        """extra_env values are merged into the subprocess environment."""
        ex = PackerExecutor(working_dir=working_dir, env_vars={"FROM_INIT": "1"})
        recorder = StreamRecorder(returncode=0)
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        ex.build("template.pkr.hcl", extra_env={"EXTRA": "yes"})

        env = recorder.calls[0]["env"]
        assert env["FROM_INIT"] == "1"
        assert env["EXTRA"] == "yes"
        assert env["PACKER_LOG"] == "1"

    def test_build_exception_returns_false_with_message(self, mocker, executor):
        """build catches exceptions and returns (False, str(e))."""
        recorder = StreamRecorder(raise_exc=RuntimeError("kaboom"))
        mocker.patch("app.services.packer_executor._stream_subprocess", recorder)

        success, stdout = executor.build("template.pkr.hcl")

        assert success is False
        assert stdout == "kaboom"


# ---------------------------------------------------------------------------
# _extract_error_from_packer()
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExtractErrorFromPacker:
    """Behaviour of ``PackerExecutor._extract_error_from_packer``."""

    def test_filters_trace_debug_and_plugingetter_noise(self, executor):
        """TRACE/DEBUG and plugingetter lines are skipped entirely."""
        stderr = (
            "[TRACE] some trace noise\n"
            "[DEBUG] some debug noise\n"
            "plugingetter: doing something\n"
            "Error: real problem\n"
        )

        msg = executor._extract_error_from_packer(stderr)

        assert "trace" not in msg.lower()
        assert "debug" not in msg.lower()
        assert "plugingetter" not in msg
        assert "Error: real problem" in msg

    def test_picks_star_get_lines(self, executor):
        """Lines containing '* Get' are kept as errors."""
        stderr = "[DEBUG] x\n* Get https://example.com failed\n"

        msg = executor._extract_error_from_packer(stderr)

        assert "* Get https://example.com failed" in msg

    def test_picks_uppercase_error_lines(self, executor):
        """Lines containing 'Error' are kept as errors."""
        stderr = "Error: invalid template syntax\n"

        msg = executor._extract_error_from_packer(stderr)

        assert "Error: invalid template syntax" in msg

    def test_picks_lowercase_error_lines(self, executor):
        """Lines containing lowercase 'error' are kept as errors."""
        stderr = "an error happened during build\n"

        msg = executor._extract_error_from_packer(stderr)

        assert "an error happened during build" in msg

    def test_picks_lines_starting_with_star(self, executor):
        """Lines whose stripped form starts with '*' are kept."""
        stderr = "   * something went wrong\n"

        msg = executor._extract_error_from_packer(stderr)

        assert "* something went wrong" in msg

    def test_joins_first_three_errors_with_pipe(self, executor):
        """When several error lines exist, the first 3 are joined with ' | '."""
        stderr = "Error: one\n" "Error: two\n" "Error: three\n" "Error: four\n"

        msg = executor._extract_error_from_packer(stderr)

        parts = msg.split(" | ")
        assert len(parts) == 3
        assert parts == ["Error: one", "Error: two", "Error: three"]

    def test_fallback_to_last_non_trace_line(self, executor):
        """When no error pattern matched, falls back to last non-TRACE/DEBUG line."""
        stderr = "[TRACE] noise\n" "informational message\n" "[TRACE] later noise\n"

        msg = executor._extract_error_from_packer(stderr)

        assert msg == "informational message"

    def test_unknown_error_when_only_trace_lines(self, executor):
        """When stderr only has TRACE/DEBUG lines, returns 'Unknown error'."""
        stderr = "[TRACE] only\n[DEBUG] noise\n"

        msg = executor._extract_error_from_packer(stderr)

        assert msg == "Unknown error"

    def test_unknown_error_when_stderr_blank(self, executor):
        """Empty stderr yields 'Unknown error'."""
        msg = executor._extract_error_from_packer("")

        assert msg == "Unknown error"
