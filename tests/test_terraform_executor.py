"""Tests for the Terraform executor service."""

import os
import subprocess
from unittest.mock import MagicMock

import pytest

from app.services import terraform_executor as te_mod
from app.services.terraform_executor import TerraformExecutor, _pg_backend_override_hcl, _stream_subprocess

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
            raise subprocess.TimeoutExpired(cmd="terraform", timeout=timeout)
        return self._returncode


# ---------------------------------------------------------------------------
# _pg_backend_override_hcl
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPgBackendOverrideHcl:
    """Verify the HCL renderer for the pg backend override file."""

    def test_renders_schema_name_in_block(self):
        """schema_name is embedded inside the backend "pg" block."""
        out = _pg_backend_override_hcl("deployment_abc")
        assert 'backend "pg"' in out
        assert 'schema_name = "deployment_abc"' in out
        assert out.endswith("\n")

    def test_escapes_double_quotes_in_schema_name(self):
        """Double-quotes inside schema_name are HCL-escaped."""
        out = _pg_backend_override_hcl('weird"name')
        assert 'schema_name = "weird\\"name"' in out


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
            ["terraform", "init"],
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
            ["terraform", "plan"],
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
            ["terraform", "init"],
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
            ["terraform", "apply"],
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
            ["terraform", "apply"],
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
# TerraformExecutor: _get_env / _write_pg_backend_override
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetEnv:
    """Verify _get_env merging rules for env_vars, extra_env, TF_LOG, PG_CONN_STR."""

    def test_env_vars_layered_on_os_environ(self, mocker, tmp_path):
        """env_vars merge on top of os.environ; os.environ keys still present."""
        mocker.patch.dict(os.environ, {"FROM_OS": "yes"}, clear=False)
        ex = TerraformExecutor(str(tmp_path), env_vars={"CUSTOM": "v"})
        env = ex._get_env()
        assert env["FROM_OS"] == "yes"
        assert env["CUSTOM"] == "v"

    def test_extra_env_wins_over_env_vars(self, mocker, tmp_path):
        """extra_env passed to _get_env overrides instance env_vars."""
        ex = TerraformExecutor(str(tmp_path), env_vars={"K": "instance"})
        env = ex._get_env(extra_env={"K": "extra"})
        assert env["K"] == "extra"

    def test_tf_log_set_when_worker_tf_log_present(self, mocker, tmp_path):
        """TF_LOG is propagated from WORKER_TF_LOG when set."""
        mocker.patch.dict(os.environ, {"WORKER_TF_LOG": "TRACE"}, clear=False)
        ex = TerraformExecutor(str(tmp_path))
        env = ex._get_env()
        assert env["TF_LOG"] == "TRACE"

    def test_tf_log_popped_when_worker_tf_log_absent(self, mocker, tmp_path):
        """TF_LOG is stripped from env if WORKER_TF_LOG is unset, even when inherited."""
        # Ensure WORKER_TF_LOG is absent and TF_LOG inherited from os.environ.
        env_copy = {k: v for k, v in os.environ.items() if k != "WORKER_TF_LOG"}
        env_copy["TF_LOG"] = "DEBUG"
        mocker.patch.dict(os.environ, env_copy, clear=True)
        ex = TerraformExecutor(str(tmp_path))
        env = ex._get_env()
        assert "TF_LOG" not in env

    def test_pg_conn_str_added_only_when_backend_conn_set(self, tmp_path):
        """PG_CONN_STR is injected only when backend_conn_str is configured."""
        ex_with = TerraformExecutor(str(tmp_path), backend_conn_str="postgres://x")
        assert ex_with._get_env()["PG_CONN_STR"] == "postgres://x"

        ex_without = TerraformExecutor(str(tmp_path))
        assert "PG_CONN_STR" not in ex_without._get_env()


@pytest.mark.unit
class TestWritePgBackendOverride:
    """Verify pg_backend_override.tf is written only when schema_name is configured."""

    def test_noop_without_schema(self, tmp_path):
        """No file is written when backend_schema_name is None."""
        ex = TerraformExecutor(str(tmp_path))
        ex._write_pg_backend_override()
        assert not (tmp_path / "pg_backend_override.tf").exists()

    def test_writes_override_with_schema(self, tmp_path):
        """Override file is written and contains the rendered HCL for the schema."""
        ex = TerraformExecutor(str(tmp_path), backend_schema_name="deploy_xyz")
        ex._write_pg_backend_override()
        override = tmp_path / "pg_backend_override.tf"
        assert override.exists()
        text = override.read_text()
        assert 'schema_name = "deploy_xyz"' in text
        assert 'backend "pg"' in text


# ---------------------------------------------------------------------------
# TerraformExecutor: init/plan/apply/destroy (mock _stream_subprocess)
# ---------------------------------------------------------------------------


def _patch_stream(mocker, returncode=0, stdout="ok", stderr=""):
    """Patch the module-level _stream_subprocess used by TerraformExecutor."""
    return mocker.patch.object(
        te_mod,
        "_stream_subprocess",
        return_value=(returncode, stdout, stderr),
    )


@pytest.mark.unit
class TestInit:
    """Verify terraform init command shape and success/failure handling."""

    def test_init_without_schema_omits_reconfigure(self, mocker, tmp_path):
        """When no schema is configured, -reconfigure is not added and no override file is written."""
        stream = _patch_stream(mocker, returncode=0, stdout="initialized")
        ex = TerraformExecutor(str(tmp_path))

        ok, stdout, stderr = ex.init()

        assert ok is True
        assert stdout == "initialized"
        assert stderr == ""
        cmd = stream.call_args.args[0]
        assert cmd[-3:] == [ex.terraform_path, "init", "-input=false"] or (cmd[1:] == ["init", "-input=false"])
        assert "-reconfigure" not in cmd
        assert stream.call_args.kwargs["timeout"] == 300
        assert stream.call_args.kwargs["tool_name"] == "terraform_init"
        assert not (tmp_path / "pg_backend_override.tf").exists()

    def test_init_with_schema_appends_reconfigure_and_writes_override(self, mocker, tmp_path):
        """With backend_schema_name, init adds -reconfigure and writes the override file."""
        _patch_stream(mocker, returncode=0)
        ex = TerraformExecutor(
            str(tmp_path),
            backend_conn_str="postgres://x",
            backend_schema_name="deploy_s",
        )
        ok, _, _ = ex.init()
        assert ok is True
        assert (tmp_path / "pg_backend_override.tf").exists()

    def test_init_failure_returns_false(self, mocker, tmp_path):
        """Non-zero return from the stream surfaces as success=False."""
        _patch_stream(mocker, returncode=1, stdout="err", stderr="")
        ex = TerraformExecutor(str(tmp_path))
        ok, stdout, _ = ex.init()
        assert ok is False
        assert stdout == "err"

    def test_init_exception_caught_returns_false(self, mocker, tmp_path):
        """An exception inside the init body returns (False, "", str(e))."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=RuntimeError("boom"))
        ex = TerraformExecutor(str(tmp_path))
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
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.plan()
        assert ok is True
        cmd = stream.call_args.args[0]
        # ``-lock=false``: per-Deployment-Schema isoliert den State, das
        # geteilte pg-Lock würde sonst unzusammenhängende Deployments
        # serialisieren (siehe TerraformExecutor.plan).
        assert cmd == [ex.terraform_path, "plan", "-input=false", "-lock=false"]
        assert stream.call_args.kwargs["timeout"] == 300

    def test_plan_with_var_file_and_variables(self, mocker, tmp_path):
        """plan with var_file appends -var-file and one -var pair per variable."""
        stream = _patch_stream(mocker, returncode=0)
        ex = TerraformExecutor(str(tmp_path))
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
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.plan()
        assert ok is False

    def test_plan_exception_returns_false(self, mocker, tmp_path):
        """An exception in plan returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=ValueError("bad"))
        ex = TerraformExecutor(str(tmp_path))
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
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.apply()
        assert ok is True
        cmd = stream.call_args.args[0]
        assert cmd == [ex.terraform_path, "apply", "-auto-approve", "-input=false", "-lock=false"]
        assert stream.call_args.kwargs["timeout"] == 1800

    def test_apply_with_targets_and_replaces(self, mocker, tmp_path):
        """apply propagates -var-file, -var, -target, -replace flags as separate args."""
        stream = _patch_stream(mocker, returncode=0)
        ex = TerraformExecutor(str(tmp_path))
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
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.apply()
        assert ok is False

    def test_apply_exception(self, mocker, tmp_path):
        """An exception in apply returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=RuntimeError("kapow"))
        ex = TerraformExecutor(str(tmp_path))
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
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.destroy()
        assert ok is True
        cmd = stream.call_args.args[0]
        assert cmd == [ex.terraform_path, "destroy", "-auto-approve", "-input=false", "-lock=false"]
        assert stream.call_args.kwargs["timeout"] == 1800

    def test_destroy_with_var_file_and_variables(self, mocker, tmp_path):
        """destroy adds -var-file and -var pairs."""
        stream = _patch_stream(mocker, returncode=0)
        ex = TerraformExecutor(str(tmp_path))
        ex.destroy(var_file="vars.tfvars", variables={"x": "9"})
        cmd = stream.call_args.args[0]
        assert "-var-file" in cmd and "vars.tfvars" in cmd
        assert "-var" in cmd and "x=9" in cmd

    def test_destroy_failure(self, mocker, tmp_path):
        """destroy non-zero rc surfaces as success=False."""
        _patch_stream(mocker, returncode=1)
        ex = TerraformExecutor(str(tmp_path))
        ok, _, _ = ex.destroy()
        assert ok is False

    def test_destroy_exception(self, mocker, tmp_path):
        """An exception in destroy returns (False, "", message)."""
        mocker.patch.object(te_mod, "_stream_subprocess", side_effect=OSError("io"))
        ex = TerraformExecutor(str(tmp_path))
        ok, stdout, stderr = ex.destroy()
        assert ok is False
        assert stdout == ""
        assert "io" in stderr


# ---------------------------------------------------------------------------
# TerraformExecutor: output / state_pull (mock subprocess.run)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOutput:
    """Verify terraform output JSON parsing and failure handling."""

    def test_output_success_returns_parsed_dict(self, mocker, tmp_path):
        """output() parses JSON stdout into a dict on success."""
        fake_result = MagicMock(returncode=0, stdout='{"ip": {"value": "1.2.3.4"}}', stderr="")
        run = mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)

        ex = TerraformExecutor(str(tmp_path))
        result = ex.output()

        assert result == {"ip": {"value": "1.2.3.4"}}
        cmd = run.call_args.args[0]
        assert cmd == [ex.terraform_path, "output", "-json"]

    def test_output_nonzero_returncode_returns_none(self, mocker, tmp_path):
        """output() returns None when terraform exits non-zero."""
        fake_result = MagicMock(returncode=1, stdout="", stderr="oops")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = TerraformExecutor(str(tmp_path))
        assert ex.output() is None

    def test_output_invalid_json_returns_none(self, mocker, tmp_path):
        """output() returns None when stdout is not valid JSON."""
        fake_result = MagicMock(returncode=0, stdout="not json", stderr="")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = TerraformExecutor(str(tmp_path))
        assert ex.output() is None

    def test_output_subprocess_raises_returns_none(self, mocker, tmp_path):
        """output() returns None when subprocess.run raises."""
        mocker.patch.object(te_mod.subprocess, "run", side_effect=RuntimeError("boom"))
        ex = TerraformExecutor(str(tmp_path))
        assert ex.output() is None


@pytest.mark.unit
class TestStatePull:
    """Verify terraform state pull return semantics."""

    def test_state_pull_success_returns_stdout_verbatim(self, mocker, tmp_path):
        """state_pull() returns stdout verbatim on rc=0."""
        fake_result = MagicMock(returncode=0, stdout='{"version":4}', stderr="")
        run = mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = TerraformExecutor(str(tmp_path))
        out = ex.state_pull()
        assert out == '{"version":4}'
        cmd = run.call_args.args[0]
        assert cmd == [ex.terraform_path, "state", "pull"]

    def test_state_pull_nonzero_returns_none(self, mocker, tmp_path):
        """state_pull() returns None on non-zero return code."""
        fake_result = MagicMock(returncode=1, stdout="ignored", stderr="bad")
        mocker.patch.object(te_mod.subprocess, "run", return_value=fake_result)
        ex = TerraformExecutor(str(tmp_path))
        assert ex.state_pull() is None

    def test_state_pull_exception_returns_none(self, mocker, tmp_path):
        """state_pull() returns None when subprocess.run raises."""
        mocker.patch.object(te_mod.subprocess, "run", side_effect=OSError("nope"))
        ex = TerraformExecutor(str(tmp_path))
        assert ex.state_pull() is None
