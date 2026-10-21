"""Tests for the OpenTofu executor service."""

import os
import subprocess
from unittest.mock import MagicMock

import pytest

from app import job_context
from app.services import opentofu_executor as te_mod
from app.services.opentofu_executor import OpenTofuExecutor, StateBackend, _stream_subprocess

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStdout:
    """Iterable stand-in for a subprocess pipe."""

    def __init__(self, lines):
        self._lines = list(lines)

    def __iter__(self):
        yield from self._lines


class FakePopen:
    """Minimal Popen double for _stream_subprocess."""

    def __init__(
        self,
        lines=None,
        returncode=0,
        raise_timeout=False,
        timeout_partial_lines=None,
    ):
        self.stdout = _FakeStdout(lines or [])
        self._returncode = returncode
        self._raise_timeout = raise_timeout
        self.pid = 4242
        self.wait_called_with = None
        # When raise_timeout is True we still want the reader thread to
        # have something to drain (the partial lines that arrived before
        # the timeout fired).
        if raise_timeout and timeout_partial_lines is not None:
            self.stdout = _FakeStdout(timeout_partial_lines)

    def wait(self, timeout=None):
        self.wait_called_with = timeout
        if self._raise_timeout:
            raise subprocess.TimeoutExpired(cmd="tofu", timeout=timeout)
        return self._returncode


# ---------------------------------------------------------------------------
# StateBackend
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStateBackend:
    """The http backend points at the API with the task's credentials."""

    def test_for_job_builds_address_and_credentials(self, mocker):
        mocker.patch.object(te_mod.settings, "APPSTORE_API_URL", "http://backend:8000/")
        backend = StateBackend.for_job("dep-1", "task-1", "tok")
        assert backend.address == "http://backend:8000/internal/tfstate/dep-1"
        env = backend.env()
        assert env["TF_HTTP_ADDRESS"] == env["TF_HTTP_LOCK_ADDRESS"] == env["TF_HTTP_UNLOCK_ADDRESS"]
        assert (env["TF_HTTP_USERNAME"], env["TF_HTTP_PASSWORD"]) == ("task-1", "tok")
        assert (env["TF_HTTP_LOCK_METHOD"], env["TF_HTTP_UNLOCK_METHOD"]) == ("LOCK", "UNLOCK")


# ---------------------------------------------------------------------------
# _stream_subprocess
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStreamSubprocess:
    """Verify _stream_subprocess streaming, timeout, and callback semantics."""

    def test_success_returns_rc_and_joined_stdout(self, mocker, tmp_path):
        """Successful run returns (rc, joined_stdout, "") and invokes the callback per line."""
        lines = ["hello\n", "world\n", "third\n"]
        fake = FakePopen(lines=lines, returncode=0)
        mocker.patch.object(te_mod.subprocess, "Popen", return_value=fake)

        seen: list[tuple[str, str]] = []

        def cb(tool, line):
            seen.append((tool, line))

        rc, stdout, stderr = _stream_subprocess(
            ["tofu", "init"],
            cwd=str(tmp_path),
            env={"FOO": "bar"},
            timeout=30,
            tool_name="terraform_init",
            output_callback=cb,
        )

        assert rc == 0
        assert stdout == "hello\nworld\nthird"
        assert stderr == ""
        assert seen == [
            ("terraform_init", "hello"),
            ("terraform_init", "world"),
            ("terraform_init", "third"),
        ]
        assert fake.wait_called_with == 30

    def test_no_callback_still_drains(self, mocker, tmp_path):
        """When output_callback is None, lines still accumulate into stdout."""
        fake = FakePopen(lines=["only-line\n"], returncode=0)
        mocker.patch.object(te_mod.subprocess, "Popen", return_value=fake)

        rc, stdout, stderr = _stream_subprocess(
            ["tofu", "plan"],
            cwd=str(tmp_path),
            env={},
            timeout=5,
            tool_name="terraform_plan",
            output_callback=None,
        )
        assert rc == 0
        assert stdout == "only-line"
        assert stderr == ""

    def test_raising_callback_is_swallowed_and_drain_continues(self, mocker, tmp_path):
        """A raising callback never aborts draining; subsequent lines still arrive."""
        lines = ["a\n", "b\n", "c\n"]
        fake = FakePopen(lines=lines, returncode=0)
        mocker.patch.object(te_mod.subprocess, "Popen", return_value=fake)

        calls: list[str] = []

        def cb(tool, line):
            calls.append(line)
            raise RuntimeError("boom")

        rc, stdout, _ = _stream_subprocess(
            ["tofu", "init"],
            cwd=str(tmp_path),
            env={},
            timeout=5,
            tool_name="terraform_init",
            output_callback=cb,
        )
        # The callback was invoked for all three lines even though each raised.
        assert calls == ["a", "b", "c"]
        # All lines made it into stdout.
        assert rc == 0
        assert stdout == "a\nb\nc"

    def test_timeout_triggers_killpg_and_returns_124(self, mocker, tmp_path):
        """A Popen.wait timeout triggers os.killpg and returns (124, partial, "Timeout")."""
        fake = FakePopen(
            lines=[],
            raise_timeout=True,
            timeout_partial_lines=["partial\n"],
        )
        mocker.patch.object(te_mod.subprocess, "Popen", return_value=fake)
        killpg = mocker.patch.object(te_mod.os, "killpg")

        rc, stdout, stderr = _stream_subprocess(
            ["tofu", "apply"],
            cwd=str(tmp_path),
            env={},
            timeout=1,
            tool_name="terraform_apply",
            output_callback=None,
        )

        assert rc == 124
        assert stderr == "Timeout"
        # stdout contains whatever was drained before timeout
        assert "partial" in stdout
        killpg.assert_called_once_with(fake.pid, 9)

    def test_timeout_swallows_killpg_oserror(self, mocker, tmp_path):
        """killpg raising OSError on timeout is swallowed; we still return the timeout tuple."""
        fake = FakePopen(lines=[], raise_timeout=True, timeout_partial_lines=[])
        mocker.patch.object(te_mod.subprocess, "Popen", return_value=fake)
        mocker.patch.object(te_mod.os, "killpg", side_effect=ProcessLookupError("gone"))

        rc, stdout, stderr = _stream_subprocess(
            ["tofu", "apply"],
            cwd=str(tmp_path),
            env={},
            timeout=1,
            tool_name="terraform_apply",
            output_callback=None,
        )
        assert rc == 124
        assert stdout == ""
        assert stderr == "Timeout"


# ---------------------------------------------------------------------------
# OpenTofuExecutor: _get_env / _write_backend_override
# ---------------------------------------------------------------------------

# What a worker's environment holds and an app's local-exec must never see.
_WORKER_SECRETS = {
    "DATABASE_URL": "postgresql://appstore_worker:pw@postgres/app",
    "CREDENTIAL_ENCRYPTION_KEY": "fernet-key",
    "GIT_ACCESS_TOKEN": "ghp_secret",
    "TFSTATE_DATABASE_URL": "postgresql://terraform:pw@postgres-tfstate/tfstate",
}


@pytest.mark.unit
class TestGetEnv:
    """The tool environment is built from scratch (job_context), not copied from the worker."""

    def test_worker_environment_is_not_inherited(self, mocker, tmp_path):
        mocker.patch.dict(os.environ, {**_WORKER_SECRETS, "FROM_OS": "yes"}, clear=False)
        env = OpenTofuExecutor(str(tmp_path), env_vars={"OS_CLOUD": "openstack"})._get_env()
        assert not set(_WORKER_SECRETS) & set(env)
        assert "FROM_OS" not in env
        assert env["OS_CLOUD"] == "openstack"
        assert env["PATH"] and env["HOME"]
        assert env["TF_IN_AUTOMATION"] == "1" and env["TF_INPUT"] == "0"

    def test_extra_env_wins_over_env_vars(self, tmp_path):
        """extra_env passed to _get_env overrides instance env_vars."""
        ex = OpenTofuExecutor(str(tmp_path), env_vars={"K": "instance"})
        env = ex._get_env(extra_env={"K": "extra"})
        assert env["K"] == "extra"

    def test_tf_log_only_from_worker_setting(self, mocker, tmp_path):
        """TF_LOG comes from WORKER_TF_LOG, never from an inherited TF_LOG."""
        mocker.patch.dict(os.environ, {"TF_LOG": "DEBUG"}, clear=False)
        mocker.patch.object(te_mod.settings, "WORKER_TF_LOG", "")
        assert "TF_LOG" not in OpenTofuExecutor(str(tmp_path))._get_env()
        mocker.patch.object(te_mod.settings, "WORKER_TF_LOG", "TRACE")
        assert OpenTofuExecutor(str(tmp_path))._get_env()["TF_LOG"] == "TRACE"

    def test_state_backend_credentials_only_with_a_backend(self, tmp_path):
        backend = StateBackend("http://api/internal/tfstate/d", "task", "tok")
        assert OpenTofuExecutor(str(tmp_path), state_backend=backend)._get_env()["TF_HTTP_PASSWORD"] == "tok"
        assert "TF_HTTP_PASSWORD" not in OpenTofuExecutor(str(tmp_path))._get_env()

    def test_slot_home_and_user_from_job_context(self, tmp_path):
        ctx = job_context.JobContext(uid=20001, home="/var/lib/appstore-worker/slot-1")
        with job_context.bind(ctx):
            env = OpenTofuExecutor(str(tmp_path))._get_env()
        assert env["HOME"] == "/var/lib/appstore-worker/slot-1"
        assert ctx.popen_kwargs()["user"] == 20001


@pytest.mark.unit
class TestWriteBackendOverride:
    """The http-backend override is written only with a state backend, and privately."""

    def test_noop_without_backend(self, tmp_path):
        OpenTofuExecutor(str(tmp_path))._write_backend_override()
        assert not (tmp_path / "appstore_backend_override.tf").exists()

    def test_writes_http_backend_without_secrets(self, tmp_path):
        backend = StateBackend("http://api/internal/tfstate/d", "task", "tok")
        OpenTofuExecutor(str(tmp_path), state_backend=backend)._write_backend_override()
        override = tmp_path / "appstore_backend_override.tf"
        text = override.read_text()
        assert 'backend "http"' in text
        assert "tok" not in text and "http://api" not in text  # env only
        assert oct(override.stat().st_mode & 0o777) == "0o600"


# ---------------------------------------------------------------------------
# OpenTofuExecutor: init/plan/apply/destroy (mock _stream_subprocess)
# ---------------------------------------------------------------------------


def _patch_stream(mocker, returncode=0, stdout="ok", stderr=""):
    """Patch the module-level _stream_subprocess used by OpenTofuExecutor."""
    return mocker.patch.object(
        te_mod,
        "_stream_subprocess",
        return_value=(returncode, stdout, stderr),
    )


@pytest.mark.unit
class TestInit:
    """Verify terraform init command shape and success/failure handling."""

    def test_init_without_backend_omits_reconfigure(self, mocker, tmp_path):
        """Without a state backend, -reconfigure is not added and no override file is written."""
        stream = _patch_stream(mocker, returncode=0, stdout="initialized")
        ex = OpenTofuExecutor(str(tmp_path))

        ok, stdout, stderr = ex.init()

        assert ok is True
        assert stdout == "initialized"
        assert stderr == ""
        cmd = stream.call_args.args[0]
        assert cmd[-3:] == [ex.tofu_path, "init", "-input=false"] or (cmd[1:] == ["init", "-input=false"])
        assert "-reconfigure" not in cmd
        assert stream.call_args.kwargs["timeout"] == 300
        assert stream.call_args.kwargs["tool_name"] == "terraform_init"
        assert not (tmp_path / "appstore_backend_override.tf").exists()

    def test_init_with_backend_appends_reconfigure_and_writes_override(self, mocker, tmp_path):
        """With a state backend, init adds -reconfigure and writes the override file."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path), state_backend=StateBackend("http://api/x", "t", "tok"))
        ok, _, _ = ex.init()
        assert ok is True
        assert "-reconfigure" in stream.call_args.args[0]
        assert (tmp_path / "appstore_backend_override.tf").exists()

    def test_init_failure_returns_false(self, mocker, tmp_path):
        """Non-zero return from the stream surfaces as success=False."""
        _patch_stream(mocker, returncode=1, stdout="err", stderr="")
        ex = OpenTofuExecutor(str(tmp_path))
        ok, stdout, _ = ex.init()
        assert ok is False
        assert stdout == "err"

    def test_init_exception_caught_returns_false(self, mocker, tmp_path):
        """An exception inside the init body returns (False, "", str(e))."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=RuntimeError("boom"))
        ex = OpenTofuExecutor(str(tmp_path))
        ok, stdout, stderr = ex.init()
        assert ok is False
        assert stdout == ""
        assert "boom" in stderr


@pytest.mark.unit
class TestPlan:
    """Verify terraform plan command shape."""

    def test_plan_basic(self, mocker, tmp_path):
        """plan with no args uses the bare command and a 300s timeout."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.plan()
        assert ok is True
        cmd = stream.call_args.args[0]
        assert cmd == [ex.tofu_path, "plan", "-input=false"]
        assert stream.call_args.kwargs["timeout"] == 300

    def test_plan_with_var_file_and_variables(self, mocker, tmp_path):
        """plan with var_file appends -var-file and one -var pair per variable."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ex.plan(var_file="vars.tfvars", variables={"a": "1", "b": "2"})
        cmd = stream.call_args.args[0]
        assert "-var-file" in cmd
        assert cmd[cmd.index("-var-file") + 1] == "vars.tfvars"
        # Each var pair appears as ["-var", "key=value"]
        for k, v in [("a", "1"), ("b", "2")]:
            assert "-var" in cmd
            assert f"{k}={v}" in cmd

    def test_plan_failure(self, mocker, tmp_path):
        """plan non-zero returncode surfaces as success=False."""
        _patch_stream(mocker, returncode=2)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.plan()
        assert ok is False

    def test_plan_exception_returns_false(self, mocker, tmp_path):
        """An exception in plan returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=ValueError("bad"))
        ex = OpenTofuExecutor(str(tmp_path))
        ok, stdout, stderr = ex.plan()
        assert ok is False
        assert stdout == ""
        assert "bad" in stderr


@pytest.mark.unit
class TestApply:
    """Verify terraform apply command shape including targets and replaces."""

    def test_apply_basic(self, mocker, tmp_path):
        """apply with no args uses the bare command and a 1800s timeout."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.apply()
        assert ok is True
        cmd = stream.call_args.args[0]
        assert cmd == [ex.tofu_path, "apply", "-auto-approve", "-input=false"]
        assert stream.call_args.kwargs["timeout"] == 1800

    def test_apply_with_targets_and_replaces(self, mocker, tmp_path):
        """apply propagates -var-file, -var, -target, -replace flags as separate args."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ex.apply(
            var_file="vars.tfvars",
            variables={"k": "v"},
            targets=['module.team_ide["Team-A"]', "openstack_compute_instance_v2.vm[0]"],
            replace=["openstack_compute_instance_v2.vm[0]"],
        )
        cmd = stream.call_args.args[0]
        assert "-var-file" in cmd and "vars.tfvars" in cmd
        assert "k=v" in cmd
        # Targets and replaces come through verbatim
        target_positions = [i for i, x in enumerate(cmd) if x == "-target"]
        assert len(target_positions) == 2
        assert cmd[target_positions[0] + 1] == 'module.team_ide["Team-A"]'
        assert cmd[target_positions[1] + 1] == "openstack_compute_instance_v2.vm[0]"
        replace_positions = [i for i, x in enumerate(cmd) if x == "-replace"]
        assert len(replace_positions) == 1
        assert cmd[replace_positions[0] + 1] == "openstack_compute_instance_v2.vm[0]"

    def test_apply_failure(self, mocker, tmp_path):
        """Non-zero apply rc surfaces as success=False."""
        _patch_stream(mocker, returncode=1)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.apply()
        assert ok is False

    def test_apply_exception(self, mocker, tmp_path):
        """An exception in apply returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=RuntimeError("kapow"))
        ex = OpenTofuExecutor(str(tmp_path))
        ok, stdout, stderr = ex.apply()
        assert ok is False
        assert stdout == ""
        assert "kapow" in stderr


@pytest.mark.unit
class TestDestroy:
    """Verify terraform destroy command shape."""

    def test_destroy_basic(self, mocker, tmp_path):
        """destroy without args uses bare command with 1800s timeout."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.destroy()
        assert ok is True
        cmd = stream.call_args.args[0]
        assert cmd == [ex.tofu_path, "destroy", "-auto-approve", "-input=false"]
        assert stream.call_args.kwargs["timeout"] == 1800

    def test_destroy_with_var_file_and_variables(self, mocker, tmp_path):
        """destroy adds -var-file and -var pairs."""
        stream = _patch_stream(mocker, returncode=0)
        ex = OpenTofuExecutor(str(tmp_path))
        ex.destroy(var_file="vars.tfvars", variables={"x": "9"})
        cmd = stream.call_args.args[0]
        assert "-var-file" in cmd and "vars.tfvars" in cmd
        assert "-var" in cmd and "x=9" in cmd

    def test_destroy_failure(self, mocker, tmp_path):
        """destroy non-zero rc surfaces as success=False."""
        _patch_stream(mocker, returncode=1)
        ex = OpenTofuExecutor(str(tmp_path))
        ok, _, _ = ex.destroy()
        assert ok is False

    def test_destroy_exception(self, mocker, tmp_path):
        """An exception in destroy returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=OSError("io"))
        ex = OpenTofuExecutor(str(tmp_path))
        ok, stdout, stderr = ex.destroy()
        assert ok is False
        assert stdout == ""
        assert "io" in stderr


# ---------------------------------------------------------------------------
# OpenTofuExecutor: output / state_pull (mock subprocess.run)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOutput:
    """Verify terraform output JSON parsing and failure handling."""

    def test_output_success_returns_parsed_dict(self, mocker, tmp_path):
        """output() parses JSON stdout into a dict on success."""
        fake_result = MagicMock(returncode=0, stdout='{"ip": {"value": "1.2.3.4"}}', stderr="")
        run = mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)

        ex = OpenTofuExecutor(str(tmp_path))
        result = ex.output()

        assert result == {"ip": {"value": "1.2.3.4"}}
        cmd = run.call_args.args[0]
        assert cmd == [ex.tofu_path, "output", "-json"]

    def test_output_nonzero_returncode_returns_none(self, mocker, tmp_path):
        """output() returns None when terraform exits non-zero."""
        fake_result = MagicMock(returncode=1, stdout="", stderr="oops")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = OpenTofuExecutor(str(tmp_path))
        assert ex.output() is None

    def test_output_invalid_json_returns_none(self, mocker, tmp_path):
        """output() returns None when stdout is not valid JSON."""
        fake_result = MagicMock(returncode=0, stdout="not json", stderr="")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = OpenTofuExecutor(str(tmp_path))
        assert ex.output() is None

    def test_output_subprocess_raises_returns_none(self, mocker, tmp_path):
        """output() returns None when subprocess.run raises."""
        mocker.patch.object(te_mod.subprocess, "run", side_effect=RuntimeError("boom"))
        ex = OpenTofuExecutor(str(tmp_path))
        assert ex.output() is None


@pytest.mark.unit
class TestStatePull:
    """Verify terraform state pull return semantics."""

    def test_state_pull_success_returns_stdout_verbatim(self, mocker, tmp_path):
        """state_pull() returns stdout verbatim on rc=0."""
        fake_result = MagicMock(returncode=0, stdout='{"version":4}', stderr="")
        run = mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = OpenTofuExecutor(str(tmp_path))
        out = ex.state_pull()
        assert out == '{"version":4}'
        cmd = run.call_args.args[0]
        assert cmd == [ex.tofu_path, "state", "pull"]

    def test_state_pull_nonzero_returns_none(self, mocker, tmp_path):
        """state_pull() returns None on non-zero return code."""
        fake_result = MagicMock(returncode=1, stdout="ignored", stderr="bad")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = OpenTofuExecutor(str(tmp_path))
        assert ex.state_pull() is None

    def test_state_pull_exception_returns_none(self, mocker, tmp_path):
        """state_pull() returns None when subprocess.run raises."""
        mocker.patch.object(te_mod.subprocess, "run", side_effect=OSError("nope"))
        ex = OpenTofuExecutor(str(tmp_path))
        assert ex.state_pull() is None


# ---------------------------------------------------------------------------
# State that could not be saved (backend unreachable at the end of an apply)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStateRescue:
    """errored.tfstate is pushed again or kept encrypted — never lost with the job directory."""

    BACKEND = StateBackend("http://api/internal/tfstate/dep-1", "task-1", "tok")

    def _executor(self, tmp_path, lines):
        return OpenTofuExecutor(
            str(tmp_path), state_backend=self.BACKEND, output_callback=lambda tool, line: lines.append(line)
        )

    def test_backend_env_asks_opentofu_to_retry(self, mocker):
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RETRY_MAX", 10)
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RETRY_WAIT_MAX", 30)
        env = self.BACKEND.env()
        assert (env["TF_HTTP_RETRY_MAX"], env["TF_HTTP_RETRY_WAIT_MIN"], env["TF_HTTP_RETRY_WAIT_MAX"]) == (
            "10",
            "1",
            "30",
        )

    def test_nothing_to_do_without_errored_state(self, mocker, tmp_path):
        stream = mocker.patch.object(te_mod, "_stream_subprocess")
        self._executor(tmp_path, [])._rescue_errored_state()
        stream.assert_not_called()

    def test_errored_state_is_pushed_once_the_backend_is_back(self, mocker, tmp_path):
        rescue_dir = tmp_path / "rescued"
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(rescue_dir))
        (tmp_path / "errored.tfstate").write_text('{"serial": 7}')
        mocker.patch.object(te_mod.time, "sleep")
        stream = mocker.patch.object(te_mod, "_stream_subprocess", side_effect=[(1, "", ""), (1, "", ""), (0, "", "")])
        lines: list[str] = []

        self._executor(tmp_path, lines)._rescue_errored_state()

        assert stream.call_count == 3
        assert stream.call_args.args[0][1:] == ["state", "push", "errored.tfstate"]
        # Same task credentials as the apply.
        assert stream.call_args.kwargs["env"]["TF_HTTP_PASSWORD"] == "tok"
        assert not (tmp_path / "errored.tfstate").exists()
        assert list(rescue_dir.iterdir()) == []  # safety copy dropped after the push
        assert any("saved to the AppStore API after all" in line for line in lines)

    def test_safety_copy_exists_before_the_first_push(self, mocker, tmp_path):
        """If the worker dies while retrying, the encrypted copy on the volume survives."""
        rescue_dir = tmp_path / "rescued"
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(rescue_dir))
        job = tmp_path / "job"
        job.mkdir()
        (job / "errored.tfstate").write_text('{"serial": 7}')
        seen: list[list[str]] = []

        def push(*args, **kwargs):
            seen.append(sorted(p.name for p in rescue_dir.iterdir()))
            raise SystemExit("worker killed")  # dies in the middle of the rescue

        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=push)
        with pytest.raises(SystemExit):
            self._executor(job, [])._rescue_errored_state()
        assert len(seen[0]) == 1 and seen[0][0].endswith(".tfstate.enc")
        assert len(list(rescue_dir.iterdir())) == 1

    def test_first_notice_already_names_the_safety_copy(self, mocker, tmp_path):
        """The first line reaches the live log at once — even if the worker dies right after."""
        rescue_dir = tmp_path / "rescued"
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(rescue_dir))
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=SystemExit("worker killed"))
        job = tmp_path / "job"
        job.mkdir()
        (job / "errored.tfstate").write_text("{}")
        lines: list[str] = []
        with pytest.raises(SystemExit):
            self._executor(job, lines)._rescue_errored_state()
        [kept] = list(rescue_dir.iterdir())
        assert str(kept) in lines[0]

    def test_pending_rescued_states_lists_undelivered_copies(self, mocker, tmp_path):
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(tmp_path / "missing"))
        assert te_mod.pending_rescued_states() == []
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(tmp_path))
        (tmp_path / "dep_task_t.tfstate.enc").write_bytes(b"x")
        (tmp_path / "other.txt").write_text("x")
        assert te_mod.pending_rescued_states() == [str(tmp_path / "dep_task_t.tfstate.enc")]

    def test_errored_state_is_kept_encrypted_when_the_backend_stays_away(self, mocker, tmp_path):
        rescue_dir = tmp_path / "rescued"
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_DIR", str(rescue_dir))
        mocker.patch.object(te_mod.settings, "WORKER_STATE_RESCUE_SECONDS", 0)
        mocker.patch.object(te_mod, "_stream_subprocess", return_value=(1, "", "connection refused"))
        job = tmp_path / "job"
        job.mkdir()
        (job / "errored.tfstate").write_text('{"serial": 7, "secret": "pw"}')
        lines: list[str] = []

        self._executor(job, lines)._rescue_errored_state()

        [kept] = list(rescue_dir.iterdir())
        assert kept.name.startswith("dep-1_task-1_") and kept.name.endswith(".tfstate.enc")
        assert oct(kept.stat().st_mode & 0o777) == "0o600"
        assert b"secret" not in kept.read_bytes()  # encrypted
        from app.utils.crypto import decrypt

        assert decrypt(kept.read_bytes()) == '{"serial": 7, "secret": "pw"}'
        assert any(str(kept) in line and "restore_tfstate.py dep-1" in line for line in lines)

    def test_apply_and_destroy_check_for_errored_state(self, mocker, tmp_path):
        mocker.patch.object(te_mod, "_stream_subprocess", return_value=(1, "Error saving state", ""))
        rescue = mocker.patch.object(OpenTofuExecutor, "_rescue_errored_state")
        ex = OpenTofuExecutor(str(tmp_path), state_backend=self.BACKEND)
        ex.apply()
        ex.destroy()
        assert rescue.call_count == 2
