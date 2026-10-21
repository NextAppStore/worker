"""The job context: minimal environment, deadline, slot user (``app/job_context.py``).

Regression for the "subprocesses inherit os.environ" finding: an app's
OpenTofu ``local-exec`` could read the Fernet key, the broker URL and the
tfstate database of every deployment from its environment.
"""

from __future__ import annotations

import os
import time

import pytest

from app import job_context
from app.services.packer_executor import PackerExecutor

pytestmark = pytest.mark.unit

WORKER_SECRETS = {
    "DATABASE_URL": "postgresql://appstore_worker:pw@postgres/app",
    "CREDENTIAL_ENCRYPTION_KEY": "fernet-key",
    "GIT_ACCESS_TOKEN": "ghp_secret",
}


def test_env_contains_only_the_minimum_plus_extra(mocker):
    mocker.patch.dict(os.environ, {**WORKER_SECRETS, "PATH": "/opt/bin:/usr/bin"}, clear=True)
    env = job_context.JobContext(home="/h").env({"OS_CLOUD": "openstack"})
    assert env == {
        "PATH": "/opt/bin:/usr/bin",
        "HOME": "/h",
        "LANG": "C.UTF-8",
        "CHECKPOINT_DISABLE": "1",
        "TF_IN_AUTOMATION": "1",
        "TF_INPUT": "0",
        "OS_CLOUD": "openstack",
    }


def test_packer_gets_no_worker_secrets(mocker, tmp_path):
    mocker.patch.dict(os.environ, WORKER_SECRETS, clear=False)
    env = PackerExecutor(str(tmp_path), env_vars={"OS_AUTH_URL": "x"})._get_env()
    assert not set(WORKER_SECRETS) & set(env)
    assert env["OS_AUTH_URL"] == "x" and env["PACKER_LOG"] == "1"


def test_timeout_is_capped_by_the_deadline_and_raises_after_it():
    ctx = job_context.JobContext(deadline=time.monotonic() + 10)
    assert ctx.timeout(3600) <= 10
    assert ctx.timeout(5) == 5
    with pytest.raises(job_context.JobTimeout):
        job_context.JobContext(deadline=time.monotonic() - 1).timeout(60)


def test_bind_sets_and_restores_the_current_context():
    ctx = job_context.JobContext(uid=20003, home="/h")
    assert job_context.current().uid is None
    with job_context.bind(ctx):
        assert job_context.current() is ctx
    assert job_context.current().uid is None


def test_popen_kwargs_drop_to_the_slot_user():
    assert job_context.JobContext().popen_kwargs() == {}
    kwargs = job_context.JobContext(uid=20001).popen_kwargs()
    assert kwargs == {"user": 20001, "group": 20001, "extra_groups": [], "umask": 0o077}


def test_write_private_is_owner_only(tmp_path):
    path = tmp_path / "secret.tfvars.json"
    job_context.JobContext().write_private(str(path), "{}")
    assert path.read_text() == "{}"
    assert oct(path.stat().st_mode & 0o777) == "0o600"


def test_for_slot_without_uid_base_only_sets_the_deadline(mocker):
    mocker.patch.object(job_context.settings, "WORKER_JOB_UID_BASE", None)
    mocker.patch.object(job_context.settings, "WORKER_JOB_TIMEOUT_SECONDS", 60)
    ctx = job_context.for_slot(2)
    assert ctx.uid is None
    assert 55 < ctx.remaining() <= 60
