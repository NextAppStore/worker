"""Branch-coverage tests for worker/app/tasks.py.

These tests mock every external boundary (TerraformExecutor, PackerExecutor,
PerTaskCloudsConfig, git_service, OpenStackService, PackerBuildLock,
packer-template discovery) so each test exercises one decision branch of the
real task code without touching disk, processes, or the network.

Celery tasks declared with ``bind=True`` are invoked here via ``task.run(...)``
— that's a method bound to the Task instance, so ``self`` inside the task
body is the real Celery task object. We patch ``task.send_event`` on a
per-test basis so the event bus is silenced.
"""

import json
import os

import pytest

from app.tasks import (
    _PHASES_WITH_PACKER,
    Failure,
    _build_current_roster,
    _looks_like_file_var_value,
    _phases_for_templates,
    _PhaseTracker,
    _reconcile_scoped_vars_to_roster,
    _scrub_nested_nones,
    _strip_file_vars,
    _tfstate_schema_name,
    deploy_application,
    destroy_deployment,
    encode_packer_vars,
    encode_terraform_vars,
    pause_deployment,
    redeploy_resource,
    resume_deployment,
)

# --- Helpers ----------------------------------------------------------------


class _FakeTemplate:
    """Stand-in for ``_PackerTemplate``: any object with a ``.key`` attr works."""

    def __init__(self, key: str) -> None:
        self.key = key


def _silence_events(mocker, task):
    """Patch ``task.send_event`` so the broker/event bus is not touched."""
    mocker.patch.object(task, "send_event", return_value=None)


def _make_clouds_config_mock(mocker):
    """Patch ``PerTaskCloudsConfig`` so __enter__ returns a fake env mapping."""
    cc = mocker.MagicMock()
    cc.__enter__ = mocker.MagicMock(return_value={"OS_CLOUD": "test"})
    cc.__exit__ = mocker.MagicMock(return_value=None)
    cls = mocker.patch("app.tasks.PerTaskCloudsConfig", return_value=cc)
    return cls, cc


def _make_terraform_executor_mock(mocker, *, init=True, plan=True, apply_=True, destroy=True, state="{}", output=None):
    """Patch ``TerraformExecutor`` so every method returns the supplied stub.

    The same instance is returned for every constructor call so the test can
    inspect call args across the whole task body.
    """
    inst = mocker.MagicMock()
    inst.init.return_value = (init, "init-stdout", "init-stderr")
    inst.plan.return_value = (plan, "plan-stdout", "plan-stderr")
    inst.apply.return_value = (apply_, "apply-stdout", "apply-stderr")
    inst.destroy.return_value = (destroy, "destroy-stdout", "destroy-stderr")
    inst.state_pull.return_value = state
    inst.output.return_value = output if output is not None else {"ip": "1.2.3.4"}
    cls = mocker.patch("app.tasks.TerraformExecutor", return_value=inst)
    return cls, inst


def _make_packer_mock(mocker, *, init=True, validate=True, build=True):
    inst = mocker.MagicMock()
    inst.init.return_value = (init, "p-stdout", "p-stderr")
    inst.validate.return_value = (validate, "p-stdout", "p-stderr")
    inst.build.return_value = (build, "build-output")
    cls = mocker.patch("app.tasks.PackerExecutor", return_value=inst)
    return cls, inst


def _make_openstack_service_mock(mocker, *, image_exists=False):
    inst = mocker.MagicMock()
    inst.check_image_exists.return_value = (image_exists, "img-id" if image_exists else None)
    inst.server_show.return_value = {"name": "vm-1", "status": "ACTIVE"}
    inst.server_stop.return_value = (True, None)
    inst.server_start.return_value = (True, None)
    cls = mocker.patch("app.tasks.OpenStackService", return_value=inst)
    return cls, inst


def _make_build_lock_mock(mocker, *, held=True):
    inst = mocker.MagicMock()
    inst.acquire_or_wait.return_value = held
    inst.release.return_value = None
    # tasks.py drives the lock via ``with PackerBuildLock(...) as lock``.
    # The real ``__enter__`` returns ``self`` (build_lock.py), so the mock
    # has to as well — a bare MagicMock would otherwise return a fresh
    # child mock and every assertion on ``inst`` would silently pass by
    # never being exercised.
    inst.__enter__.return_value = inst
    inst.__exit__.return_value = None
    cls = mocker.patch("app.tasks.PackerBuildLock", return_value=inst)
    return cls, inst


def _make_git_mock(mocker, repo_path):
    """Patch the ``git_service`` symbol used inside tasks.py."""
    gs = mocker.patch("app.tasks.git_service")
    gs.clone_release.return_value = repo_path
    gs.cleanup_repository.return_value = None
    return gs


def _patch_git_repo(mocker):
    """Patch the ``git.Repo(...)`` call inside the inline ``import git`` block."""
    fake_commit = mocker.MagicMock()
    fake_commit.hexsha = "abcdef1234567890"
    fake_commit.message = "commit msg"
    fake_commit.author = "Tester"
    fake_commit.committed_datetime.isoformat.return_value = "2024-01-01T00:00:00"
    repo_obj = mocker.MagicMock()
    repo_obj.head.commit = fake_commit

    import sys

    fake_git_mod = mocker.MagicMock()
    fake_git_mod.Repo.return_value = repo_obj
    mocker.patch.dict(sys.modules, {"git": fake_git_mod})
    return fake_commit


def _seed_terraform_dir(repo_path: str) -> str:
    """Create the ``terraform/`` subdir so ``os.path.exists`` checks pass."""
    tf_dir = os.path.join(repo_path, "terraform")
    os.makedirs(tf_dir, exist_ok=True)
    return tf_dir


# --- Pure helper unit tests -------------------------------------------------


@pytest.mark.unit
class TestEncodeTerraformVars:
    """Verify the encoding helper produces CLI-safe strings."""

    def test_dict_value_is_json_encoded(self):
        """A dict value is serialised to a JSON object literal, not str()."""
        out = encode_terraform_vars({"users": {"Team-1": [{"email": "a@b"}]}})
        # Must round-trip through JSON, not Python repr (no single quotes).
        assert json.loads(out["users"]) == {"Team-1": [{"email": "a@b"}]}

    def test_list_value_is_json_encoded(self):
        """A list value comes out as a JSON array literal."""
        out = encode_terraform_vars({"ports": [22, 80]})
        assert out["ports"] == "[22, 80]"

    def test_bool_lowercased(self):
        """HCL accepts only lowercase booleans, never Python's ``True``."""
        out = encode_terraform_vars({"flag": True, "off": False})
        assert out["flag"] == "true"
        assert out["off"] == "false"

    def test_none_top_level_dropped(self):
        """A top-level ``None`` value is omitted entirely (no -var emitted)."""
        out = encode_terraform_vars({"keep": "x", "drop": None})
        assert out == {"keep": "x"}

    def test_nested_none_scrubbed(self):
        """Nested ``None`` keys/items are stripped recursively before encoding."""
        out = encode_terraform_vars({"m": {"a": None, "b": [1, None, 2]}})
        assert json.loads(out["m"]) == {"b": [1, 2]}


@pytest.mark.unit
class TestEncodePackerVars:
    """Packer encodes typed lists as JSON arrays (not comma joins)."""

    def test_list_becomes_json_array(self):
        """A list value emits an HCL-compatible JSON array literal."""
        out = encode_packer_vars({"net": ["NAT"]})
        assert out["net"] == '["NAT"]'

    def test_dict_becomes_json_object(self):
        """A dict value emits a JSON object literal."""
        out = encode_packer_vars({"m": {"a": 1}})
        assert out["m"] == '{"a": 1}'

    def test_bool_lowercased(self):
        """Bool values match HCL's lowercase literals."""
        assert encode_packer_vars({"x": False})["x"] == "false"


@pytest.mark.unit
class TestStripFileVars:
    """File-shape vars are filtered before destroy/cleanup."""

    def test_strips_file_shape_value(self):
        """A map with content_b64 slots is recognised and removed."""
        vars_in = {
            "files": {"k": {"content_b64": "abc", "name": "a"}},
            "name": "kept",
        }
        out = _strip_file_vars(vars_in)
        assert "files" not in out
        assert out["name"] == "kept"

    def test_keeps_non_file_dict(self):
        """A regular dict-shaped variable survives the strip."""
        out = _strip_file_vars({"users": {"Team-1": ["a"]}})
        assert "users" in out

    def test_looks_like_file_var_rejects_metadata_only(self):
        """A dict with metadata but no content_b64 is NOT classified as file."""
        assert not _looks_like_file_var_value({"k": {"name": "a", "size": 1}})


@pytest.mark.unit
class TestScrubNestedNones:
    """Recursive None-scrubber walks dicts and lists."""

    def test_keeps_bool_false(self):
        """``False`` must survive scrubbing — it's a valid value, not None."""
        assert _scrub_nested_nones({"f": False}) == {"f": False}

    def test_drops_none_in_list(self):
        """``None`` entries are filtered out of lists."""
        assert _scrub_nested_nones([1, None, 2]) == [1, 2]


@pytest.mark.unit
class TestSchemaName:
    """Schema names are unquoted identifiers — hyphens become underscores."""

    def test_replaces_hyphens(self):
        """UUIDs with hyphens are converted into a single ``deployment_<safe>``."""
        out = _tfstate_schema_name("11111111-2222-3333-4444-555555555555")
        assert "-" not in out
        assert out.startswith("deployment_")


@pytest.mark.unit
class TestPhasesForTemplates:
    """Phase tuple is rebuilt depending on how many templates were discovered."""

    def test_empty_templates_drops_packer_block(self):
        """No template → ``_PHASES_WITHOUT_PACKER`` (no packer phases at all)."""
        phases = _phases_for_templates([])
        assert "PACKER_INIT" not in phases
        assert "PACKER_BUILD" not in phases

    def test_legacy_single_default_template_uses_unsuffixed_phases(self):
        """A single ``default`` template uses the legacy unsuffixed phase names."""
        phases = _phases_for_templates([_FakeTemplate("default")])
        assert phases == _PHASES_WITH_PACKER

    def test_multi_template_emits_suffixed_phases_per_template(self):
        """Multi-template repos get one ``PACKER_*:<key>`` trio per template."""
        phases = _phases_for_templates([_FakeTemplate("web"), _FakeTemplate("db")])
        assert "PACKER_INIT:web" in phases
        assert "PACKER_BUILD:db" in phases
        # Order is preserved as caller passed.
        assert phases.index("PACKER_INIT:web") < phases.index("PACKER_INIT:db")


@pytest.mark.unit
class TestPhaseTrackerUnknownPhase:
    """Unknown phases are logged but never advance the progress bar."""

    def test_mark_unknown_phase_does_not_call_progress(self, mocker):
        """An unknown phase emits a phase() log marker but no progress() event."""
        fake_logger = mocker.MagicMock()
        tracker = _PhaseTracker(fake_logger, ("A", "B"))
        tracker.mark("NOT_IN_LIST")
        fake_logger.phase.assert_called_with("NOT_IN_LIST")
        fake_logger.progress.assert_not_called()


@pytest.mark.unit
class TestFailureRoundTrip:
    """Failure exception serialises everything into args[0] for celery."""

    def test_to_dict_contains_payload(self):
        """to_dict round-trips through the JSON in ``args[0]``."""
        f = Failure("err", "dep-1", logs_dict=[{"k": "v"}], tf_state="state")
        d = f.to_dict()
        assert d["error"] == "err"
        assert d["deployment_id"] == "dep-1"
        assert d["tf_state"] == "state"

    def test_pickle_round_trip_via_reduce(self):
        """``__reduce__`` allows pickling without re-encoding the payload."""
        import pickle

        f = Failure("err", "dep-1", logs_dict=[], tf_state=None)
        restored = pickle.loads(pickle.dumps(f))
        assert isinstance(restored, Failure)
        assert restored.deployment_id == "dep-1"


@pytest.mark.unit
class TestBuildCurrentRoster:
    """Roster helper produces correct team and team-user composite keys."""

    def test_handles_dict_and_string_members(self):
        """Members may be raw email strings or dicts with an ``email`` field."""
        teams = {"T1": ["a@x"], "T2": [{"email": "b@x"}]}
        team_keys, user_keys = _build_current_roster(teams)
        assert team_keys == {"T1", "T2"}
        assert "T1-a@x" in user_keys and "T2-b@x" in user_keys


@pytest.mark.unit
class TestReconcileScopedVarsToRoster:
    """Stale scoped maps are intersected with the current roster."""

    def test_drops_orphan_slots(self, mocker):
        """A scoped var keyed by team drops keys that no longer exist."""
        fake_logger = mocker.MagicMock()
        teams = {"T1": ["a@x"]}
        vars_in = {"flavor": {"T1": "small", "T_OLD": "large"}}
        out = _reconcile_scoped_vars_to_roster(vars_in, teams, fake_logger)
        assert out["flavor"] == {"T1": "small"}

    def test_users_key_is_passed_through(self, mocker):
        """The reserved ``users`` injection is never reconciled."""
        fake_logger = mocker.MagicMock()
        teams = {"T1": ["a@x"]}
        vars_in = {"users": teams}
        out = _reconcile_scoped_vars_to_roster(vars_in, teams, fake_logger)
        assert out["users"] is teams


# --- deploy_application integration paths -----------------------------------


@pytest.mark.unit
class TestDeployApplication:
    """Branch coverage for the main deploy task body."""

    def test_happy_path_legacy_single_template(self, mocker, tmp_path):
        """Single ``default`` template -> packer init+validate+build then terraform plan/apply."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("default")])
        _make_openstack_service_mock(mocker, image_exists=False)
        _, lock_inst = _make_build_lock_mock(mocker, held=True)
        _, packer_inst = _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        result = deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"packer": {"flavor": "m1.small"}, "terraform": {"region": "eu"}},
            teams={"T1": [{"email": "a@x"}]},
            openstack_envelope={"project_id": "p1"},
        )

        assert result["status"] == "success"
        assert result["deployment_id"] == "dep-1"
        assert result["terraform_outputs"] == {"ip": "1.2.3.4"}
        assert packer_inst.init.called
        assert packer_inst.build.called
        assert tf_inst.plan.called and tf_inst.apply.called
        assert not tf_inst.destroy.called
        lock_inst.acquire_or_wait.assert_called_once()
        # Teardown now runs through the context-manager protocol.
        lock_inst.__exit__.assert_called_once()

    def test_multi_template_iterates_each_discovered(self, mocker, tmp_path):
        """Multi-template repo runs Packer once per template (in order)."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("web"), _FakeTemplate("db")])
        _make_openstack_service_mock(mocker, image_exists=False)
        _make_build_lock_mock(mocker, held=True)
        _, packer_inst = _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        result = deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"packer": {"web": {"f": "x"}, "db": {"f": "y"}}, "terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )

        assert result["status"] == "success"
        assert packer_inst.build.call_count == 2
        applied_vars = tf_inst.apply.call_args.kwargs["variables"]
        assert "image_name_web" in applied_vars
        assert "image_name_db" in applied_vars

    def test_skip_packer_when_image_exists(self, mocker, tmp_path):
        """check_image_exists True → packer methods never invoked, terraform still runs."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("default")])
        _make_openstack_service_mock(mocker, image_exists=True)
        _, lock_inst = _make_build_lock_mock(mocker)
        _, packer_inst = _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        result = deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )

        assert result["status"] == "success"
        packer_inst.init.assert_not_called()
        packer_inst.build.assert_not_called()
        lock_inst.acquire_or_wait.assert_not_called()
        assert tf_inst.apply.called

    def test_build_lock_wait_then_image_appears(self, mocker, tmp_path):
        """acquire_or_wait False once → loop announces waiting then notices image arrived."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("default")])

        os_inst = mocker.MagicMock()
        os_inst.check_image_exists.side_effect = [(False, None), (True, "id-x")]
        mocker.patch("app.tasks.OpenStackService", return_value=os_inst)

        lock_inst = mocker.MagicMock()
        lock_inst.acquire_or_wait.return_value = False  # blocked → wait branch
        lock_inst.__enter__.return_value = lock_inst
        lock_inst.__exit__.return_value = None
        mocker.patch("app.tasks.PackerBuildLock", return_value=lock_inst)

        _, packer_inst = _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        result = deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )

        assert result["status"] == "success"
        packer_inst.build.assert_not_called()
        assert tf_inst.apply.called

    def test_packer_build_failure_skips_terraform(self, mocker, tmp_path):
        """A failing ``packer.build`` raises before any terraform method runs."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("default")])
        _make_openstack_service_mock(mocker, image_exists=False)
        _make_build_lock_mock(mocker, held=True)
        _make_packer_mock(mocker, build=False)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        with pytest.raises(Failure):
            deploy_application.run(
                deployment_id="dep-1",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={"terraform": {}},
                teams=None,
                openstack_envelope={"project_id": "p1"},
            )

        # Terraform init/plan/apply must NOT have been called.
        tf_inst.init.assert_not_called()
        tf_inst.apply.assert_not_called()

    def test_terraform_apply_failure_triggers_cleanup_destroy(self, mocker, tmp_path):
        """A non-zero ``terraform apply`` raises Failure and attempts cleanup destroy."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _make_openstack_service_mock(mocker)
        _make_build_lock_mock(mocker)
        _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker, apply_=False)

        with pytest.raises(Failure) as exc:
            deploy_application.run(
                deployment_id="dep-2",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={"terraform": {}},
                teams=None,
                openstack_envelope={"project_id": "p1"},
            )

        assert "Terraform apply failed" in str(exc.value)
        assert tf_inst.destroy.called  # cleanup-after-failure
        assert tf_inst.apply.called

    def test_missing_envelope_raises_failure(self, mocker, tmp_path):
        """No ``openstack_envelope`` short-circuits with a Failure exception."""
        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, str(tmp_path / "repo"))
        with pytest.raises(Failure) as exc:
            deploy_application.run(
                deployment_id="dep-x",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={"terraform": {}},
                teams=None,
                openstack_envelope=None,
            )
        assert "envelope" in str(exc.value).lower()

    def test_no_plaintext_secret_in_logs(self, mocker, tmp_path):
        """No secret value from openstack_envelope leaks into the captured logs."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _make_openstack_service_mock(mocker)
        _make_build_lock_mock(mocker)
        _make_packer_mock(mocker)
        _make_terraform_executor_mock(mocker)

        secret = "SUPER-SECRET-PASSWORD-9999"
        result = deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={
                "project_id": "p1",
                "password": secret,  # opaque encrypted blob in real life
            },
        )
        serialised = json.dumps(result["logs"])
        assert secret not in serialised

    def test_variable_encoding_dict_passed_as_json(self, mocker, tmp_path):
        """A dict in user_vars["terraform"] reaches terraform as JSON string."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, deploy_application)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _make_openstack_service_mock(mocker)
        _make_build_lock_mock(mocker)
        _make_packer_mock(mocker)
        _, tf_inst = _make_terraform_executor_mock(mocker)

        deploy_application.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {"mapping": {"k": "v"}, "list": [1, 2]}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )
        applied_vars = tf_inst.apply.call_args.kwargs["variables"]
        assert json.loads(applied_vars["mapping"]) == {"k": "v"}
        assert applied_vars["list"] == "[1, 2]"


# --- destroy_deployment -----------------------------------------------------


@pytest.mark.unit
class TestDestroyDeployment:
    """Branches in the destroy task body."""

    def test_happy_path_emits_success(self, mocker, tmp_path):
        """A successful destroy returns status=success and calls terraform.destroy."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, destroy_deployment)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _, tf_inst = _make_terraform_executor_mock(mocker)

        result = destroy_deployment.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )
        assert result["status"] == "success"
        assert result["terraform_outputs"] == {}
        assert tf_inst.destroy.called

    def test_destroy_failure_raises_failure(self, mocker, tmp_path):
        """A failing ``terraform destroy`` raises Failure with destroy-failed message."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, destroy_deployment)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _make_terraform_executor_mock(mocker, destroy=False)

        with pytest.raises(Failure) as exc:
            destroy_deployment.run(
                deployment_id="dep-1",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={"terraform": {}},
                teams=None,
                openstack_envelope={"project_id": "p1"},
            )
        assert "Terraform destroy failed" in str(exc.value)


# --- pause / resume ---------------------------------------------------------


@pytest.mark.unit
class TestPauseResume:
    """Pause/resume share a body. Verify both call the right OpenStack method."""

    def _run(self, mocker, tmp_path, task, *, server_op):
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, task)
        _make_git_mock(mocker, repo_path)
        _make_clouds_config_mock(mocker)
        state = json.dumps(
            {
                "resources": [
                    {
                        "type": "openstack_compute_instance_v2",
                        "instances": [
                            {"attributes": {"id": "srv-1", "name": "web"}},
                            {"attributes": {"id": "srv-2", "name": "db"}},
                        ],
                    }
                ]
            }
        )
        _make_terraform_executor_mock(mocker, state=state)
        _, os_inst = _make_openstack_service_mock(mocker)

        result = task.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={},
            teams=None,
            openstack_envelope={"project_id": "p1"},
        )
        assert result["status"] == "success"
        method = getattr(os_inst, f"server_{server_op}")
        assert method.call_count == 2

    def test_pause_calls_server_stop_per_vm(self, mocker, tmp_path):
        """Pause runs ``server_stop`` once for each server discovered in state."""
        self._run(mocker, tmp_path, pause_deployment, server_op="stop")

    def test_resume_calls_server_start_per_vm(self, mocker, tmp_path):
        """Resume runs ``server_start`` once for each server discovered in state."""
        self._run(mocker, tmp_path, resume_deployment, server_op="start")

    def test_pause_no_servers_raises_failure(self, mocker, tmp_path):
        """Empty state → no servers found → Failure (deploy never reached apply)."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, pause_deployment)
        _make_git_mock(mocker, repo_path)
        _make_clouds_config_mock(mocker)
        _make_terraform_executor_mock(mocker, state='{"resources": []}')
        _, os_inst = _make_openstack_service_mock(mocker)

        with pytest.raises(Failure) as exc:
            pause_deployment.run(
                deployment_id="dep-1",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={},
                teams=None,
                openstack_envelope={"project_id": "p1"},
            )
        assert "No compute instances" in str(exc.value)
        os_inst.server_stop.assert_not_called()

    def test_pause_partial_failure_surfaces_ids(self, mocker, tmp_path):
        """If a single server fails to stop, the error message names it."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, pause_deployment)
        _make_git_mock(mocker, repo_path)
        _make_clouds_config_mock(mocker)
        state = json.dumps(
            {
                "resources": [
                    {
                        "type": "openstack_compute_instance_v2",
                        "instances": [
                            {"attributes": {"id": "ok-1"}},
                            {"attributes": {"id": "bad-1"}},
                        ],
                    }
                ]
            }
        )
        _make_terraform_executor_mock(mocker, state=state)
        os_inst = mocker.MagicMock()
        os_inst.server_show.return_value = {"name": "n", "status": "ACTIVE"}
        os_inst.server_stop.side_effect = [(True, None), (False, "locked")]
        mocker.patch("app.tasks.OpenStackService", return_value=os_inst)

        with pytest.raises(Failure) as exc:
            pause_deployment.run(
                deployment_id="dep-1",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={},
                teams=None,
                openstack_envelope={"project_id": "p1"},
            )
        msg = str(exc.value)
        assert "bad-1" in msg
        assert "locked" in msg


# --- redeploy_resource ------------------------------------------------------


@pytest.mark.unit
class TestRedeployResource:
    """Per-VM redeploy uses ``-target`` and ``-replace``."""

    def test_apply_called_with_target_and_replace(self, mocker, tmp_path):
        """terraform.apply receives the resource address in both targets and replace."""
        repo_path = str(tmp_path / "repo")
        os.makedirs(repo_path)
        _seed_terraform_dir(repo_path)

        _silence_events(mocker, redeploy_resource)
        _make_git_mock(mocker, repo_path)
        _patch_git_repo(mocker)
        _make_clouds_config_mock(mocker)
        mocker.patch("app.tasks._discover_packer_templates", return_value=[])
        _, tf_inst = _make_terraform_executor_mock(mocker)

        addr = 'openstack_compute_instance_v2.team_ide["Team-A"]'
        result = redeploy_resource.run(
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
            resource_address=addr,
        )
        assert result["status"] == "success"
        kwargs = tf_inst.apply.call_args.kwargs
        assert kwargs["targets"] == [addr]
        assert kwargs["replace"] == [addr]

    def test_invalid_address_short_circuits(self, mocker, tmp_path):
        """A malformed resource_address fails fast before any clone or terraform call."""
        _silence_events(mocker, redeploy_resource)
        _make_git_mock(mocker, str(tmp_path / "repo"))
        with pytest.raises(Failure) as exc:
            redeploy_resource.run(
                deployment_id="dep-1",
                app_id="myapp",
                app_git_link="https://git/repo.git",
                release="v1",
                user_vars={"terraform": {}},
                teams=None,
                openstack_envelope={"project_id": "p1"},
                resource_address="; rm -rf /",
            )
        assert "invalid resource_address" in str(exc.value)
