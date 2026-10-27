"""Tests for the OpenStack CLI wrapper service."""

import json
import subprocess

import pytest

from app.services.openstack_service import OpenStackService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _CompletedStub:
    """Minimal stand-in for subprocess.CompletedProcess."""

    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def env_vars():
    """Per-task OpenStack env vars."""
    return {
        "OS_AUTH_URL": "https://keystone.example.com:5000/v3",
        "OS_USERNAME": "test-user",
        "OS_PASSWORD": "secret",
        "OS_PROJECT_NAME": "demo",
    }


@pytest.fixture
def service(env_vars):
    """OpenStackService configured with sample credentials."""
    return OpenStackService(env_vars=env_vars)


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRun:
    """Tests for the internal _run helper."""

    def test_run_returns_triple_on_success(self, service, mocker):
        """_run returns (returncode, stdout, stderr) verbatim when subprocess succeeds."""
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="ok\n", stderr=""),
        )
        rc, out, err = service._run(["openstack", "image", "list"], timeout=30)
        assert (rc, out, err) == (0, "ok\n", "")
        mock_sp.assert_called_once()
        kwargs = mock_sp.call_args.kwargs
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert kwargs["timeout"] == 30

    def test_run_passes_only_the_job_environment(self, service, mocker):
        """_run builds the env from scratch: PATH plus the job's OS_* values, nothing of the worker."""
        mocker.patch.dict(
            "os.environ",
            {
                "PATH": "/usr/bin",
                "OS_AUTH_URL": "should-be-overridden",
                "DATABASE_URL": "postgresql://appstore_worker:pw@postgres/app",
                "CREDENTIAL_ENCRYPTION_KEY": "fernet-key",
            },
            clear=True,
        )
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="", stderr=""),
        )
        service._run(["openstack", "image", "list"])
        env_passed = mock_sp.call_args.kwargs["env"]
        # PATH so the CLI is found; the worker's secrets are not passed on.
        assert env_passed["PATH"] == "/usr/bin"
        assert "DATABASE_URL" not in env_passed
        assert "CREDENTIAL_ENCRYPTION_KEY" not in env_passed
        # env_vars override and add OS_* values
        assert env_passed["OS_AUTH_URL"] == "https://keystone.example.com:5000/v3"
        assert env_passed["OS_USERNAME"] == "test-user"
        assert env_passed["OS_PASSWORD"] == "secret"
        assert env_passed["OS_PROJECT_NAME"] == "demo"

    def test_run_converts_none_stdout_stderr_to_empty_string(self, service, mocker):
        """_run coerces None stdout/stderr (e.g. when text=False) to empty strings."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout=None, stderr=None),
        )
        rc, out, err = service._run(["openstack", "image", "list"])
        assert rc == 0
        assert out == ""
        assert err == ""

    def test_run_handles_timeout_expired(self, service, mocker):
        """_run returns (-1, '', timeout message) when subprocess.TimeoutExpired is raised."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="openstack", timeout=30),
        )
        rc, out, err = service._run(["openstack", "image", "list"], timeout=30)
        assert rc == -1
        assert out == ""
        assert "Timeout after 30s" in err
        assert "openstack image list" in err

    def test_run_handles_file_not_found(self, service, mocker):
        """_run returns a helpful message when the openstack binary is missing."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            side_effect=FileNotFoundError(),
        )
        rc, out, err = service._run(["openstack", "image", "list"])
        assert rc == -1
        assert out == ""
        assert "OpenStack CLI not found" in err

    def test_run_handles_generic_exception(self, service, mocker):
        """_run wraps any other exception into a (-1, '', message) tuple."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            side_effect=RuntimeError("boom"),
        )
        rc, out, err = service._run(["openstack", "image", "list"])
        assert rc == -1
        assert out == ""
        assert "Error running openstack CLI" in err
        assert "boom" in err


# ---------------------------------------------------------------------------
# check_image_exists
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckImageExists:
    """Tests for the image-existence check."""

    def test_returns_false_when_no_auth_url_and_skips_subprocess(self, mocker):
        """Returns (False, None) without invoking the CLI when OS_AUTH_URL is absent."""
        mock_sp = mocker.patch("app.services.openstack_service.subprocess.run")
        svc = OpenStackService(env_vars={"OS_USERNAME": "x"})
        exists, image_id = svc.check_image_exists("my-image")
        assert exists is False
        assert image_id is None
        mock_sp.assert_not_called()

    def test_returns_false_when_env_vars_is_none(self, mocker):
        """Returns (False, None) without invoking the CLI when env_vars is None."""
        mock_sp = mocker.patch("app.services.openstack_service.subprocess.run")
        svc = OpenStackService(env_vars=None)
        assert svc.check_image_exists("my-image") == (False, None)
        mock_sp.assert_not_called()

    def test_returns_false_when_rc_nonzero(self, service, mocker):
        """Returns (False, None) when the CLI exits non-zero."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=1, stdout="", stderr="auth failed"),
        )
        assert service.check_image_exists("my-image") == (False, None)

    def test_returns_false_on_invalid_json(self, service, mocker):
        """Returns (False, None) when CLI stdout is not valid JSON."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="not-json", stderr=""),
        )
        assert service.check_image_exists("my-image") == (False, None)

    def test_returns_false_on_empty_list(self, service, mocker):
        """Returns (False, None) when CLI returns an empty JSON list."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="[]", stderr=""),
        )
        assert service.check_image_exists("my-image") == (False, None)

    def test_returns_true_with_single_image(self, service, mocker):
        """Returns (True, image_id) when exactly one image matches."""
        payload = json.dumps([{"ID": "img-1", "Name": "my-image"}])
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout=payload, stderr=""),
        )
        assert service.check_image_exists("my-image") == (True, "img-1")

    def test_returns_first_id_when_multiple_images(self, service, mocker):
        """Returns the first image's ID when multiple matches are returned."""
        payload = json.dumps(
            [
                {"ID": "img-first", "Name": "my-image"},
                {"ID": "img-second", "Name": "my-image"},
            ]
        )
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout=payload, stderr=""),
        )
        assert service.check_image_exists("my-image") == (True, "img-first")

    def test_invokes_cli_with_expected_arguments_and_env(self, service, mocker):
        """The CLI is invoked with image list, the name filter, and the merged env."""
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="[]", stderr=""),
        )
        service.check_image_exists("my-image")
        args = mock_sp.call_args.args[0]
        assert args == [
            "openstack",
            "image",
            "list",
            "--name",
            "my-image",
            "-f",
            "json",
        ]
        env_passed = mock_sp.call_args.kwargs["env"]
        assert env_passed["OS_AUTH_URL"] == "https://keystone.example.com:5000/v3"
        assert mock_sp.call_args.kwargs["timeout"] == 30


# ---------------------------------------------------------------------------
# server_show
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestServerShow:
    """Tests for the server_show wrapper."""

    def test_returns_dict_on_success(self, service, mocker):
        """Returns the parsed JSON dict when the CLI returns rc=0 and valid JSON."""
        payload = json.dumps({"id": "srv-1", "name": "web-1", "status": "ACTIVE"})
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout=payload, stderr=""),
        )
        result = service.server_show("srv-1")
        assert result == {"id": "srv-1", "name": "web-1", "status": "ACTIVE"}

    def test_returns_none_on_nonzero_rc(self, service, mocker):
        """Returns None when the CLI exits non-zero (e.g. server not found)."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=1, stdout="", stderr="No server\n"),
        )
        assert service.server_show("srv-missing") is None

    def test_returns_none_on_invalid_json(self, service, mocker):
        """Returns None when rc=0 but stdout is not valid JSON."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="<<garbage>>", stderr=""),
        )
        assert service.server_show("srv-1") is None

    def test_invokes_cli_with_show_arguments(self, service, mocker):
        """Calls the CLI with the expected ``server show`` argument list."""
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="{}", stderr=""),
        )
        service.server_show("srv-1")
        assert mock_sp.call_args.args[0] == [
            "openstack",
            "server",
            "show",
            "srv-1",
            "-f",
            "json",
        ]
        assert mock_sp.call_args.kwargs["timeout"] == 30


# ---------------------------------------------------------------------------
# server_stop
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestServerStop:
    """Tests for the server_stop wrapper."""

    def test_returns_success_when_rc_zero(self, service, mocker):
        """Returns (True, None) when stop exits 0."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="", stderr=""),
        )
        assert service.server_stop("srv-1") == (True, None)

    def test_returns_stderr_stripped_on_failure(self, service, mocker):
        """Returns (False, stripped stderr) when stop exits non-zero."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=1, stdout="", stderr="  Instance is locked  \n"),
        )
        ok, err = service.server_stop("srv-1")
        assert ok is False
        assert err == "Instance is locked"

    def test_returns_default_message_when_stderr_empty(self, service, mocker):
        """Returns a helpful default message when stop fails with empty stderr."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=2, stdout="", stderr=""),
        )
        ok, err = service.server_stop("srv-1")
        assert ok is False
        assert err == "openstack server stop failed"

    def test_invokes_cli_with_stop_arguments_and_long_timeout(self, service, mocker):
        """Calls the CLI with ``server stop <id>`` and the 120 s timeout."""
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="", stderr=""),
        )
        service.server_stop("srv-1")
        assert mock_sp.call_args.args[0] == ["openstack", "server", "stop", "srv-1"]
        assert mock_sp.call_args.kwargs["timeout"] == 120


# ---------------------------------------------------------------------------
# server_start
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestServerStart:
    """Tests for the server_start wrapper."""

    def test_returns_success_when_rc_zero(self, service, mocker):
        """Returns (True, None) when start exits 0."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="", stderr=""),
        )
        assert service.server_start("srv-1") == (True, None)

    def test_returns_stderr_stripped_on_failure(self, service, mocker):
        """Returns (False, stripped stderr) when start exits non-zero."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=1, stdout="", stderr="\nQuota exceeded\n"),
        )
        ok, err = service.server_start("srv-1")
        assert ok is False
        assert err == "Quota exceeded"

    def test_returns_default_message_when_stderr_empty(self, service, mocker):
        """Returns a helpful default message when start fails with empty stderr."""
        mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=3, stdout="", stderr="   "),
        )
        ok, err = service.server_start("srv-1")
        assert ok is False
        assert err == "openstack server start failed"

    def test_invokes_cli_with_start_arguments_and_long_timeout(self, service, mocker):
        """Calls the CLI with ``server start <id>`` and the 120 s timeout."""
        mock_sp = mocker.patch(
            "app.services.openstack_service.subprocess.run",
            return_value=_CompletedStub(returncode=0, stdout="", stderr=""),
        )
        service.server_start("srv-1")
        assert mock_sp.call_args.args[0] == ["openstack", "server", "start", "srv-1"]
        assert mock_sp.call_args.kwargs["timeout"] == 120
