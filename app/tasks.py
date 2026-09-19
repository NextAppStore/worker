import contextlib
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, NoReturn

import git

from .celery_app import celery_app
from .config import settings
from .services import (
    OpenStackService,
    PackerBuildLock,
    PackerExecutor,
    PerTaskCloudsConfig,
    TerraformExecutor,
    git_service,
)
from .services.packer_discovery import PackerTemplateDiscoveryError, _discover_packer_templates, _PackerTemplate
from .utils.logger import LogCategory, get_logger


def _tfstate_schema_name(deployment_id: str) -> str:
    """Postgres schema name for one deployment's Terraform state.

    UUIDs contain hyphens, which would force every reference to be
    double-quoted. Replacing hyphens with underscores keeps the schema
    a plain unquoted identifier and avoids escaping hazards in any
    backend-config plumbing.
    """
    return f"deployment_{deployment_id.replace('-', '_')}"


class Failure(Exception):
    """Custom exception that carries deployment details for Celery.

    The full failure payload is serialised once into ``args[0]`` as a JSON
    string. The backend's celery event listener parses that JSON back via
    a ``Failure\\('<json>'\\)`` regex over the traceback.

    ``__reduce__`` is overridden so pickle reconstructs the exception via
    the ``_from_payload`` classmethod, which accepts the single JSON string
    directly.
    """

    def __init__(
        self,
        message: str,
        deployment_id: str,
        logs_dict: list[dict[str, Any]] | dict[str, Any],
        tf_state: str | None = None,
        commit_info: dict[str, Any] | None = None,
        terraform_outputs: dict[str, Any] | None = None,
    ):
        self.deployment_id = deployment_id
        self.logs_dict = logs_dict
        self.tf_state = tf_state
        self.commit_info = commit_info
        self.terraform_outputs = terraform_outputs

        # Encode all data as JSON in the exception message
        data = {
            "error": message,
            "deployment_id": deployment_id,
            "logs": logs_dict,
            "tf_state": tf_state,
            "commit_info": commit_info,
            "terraform_outputs": terraform_outputs,
        }
        super().__init__(json.dumps(data))

    @classmethod
    def _from_payload(cls, payload: str) -> "Failure":
        """Reconstruct a Failure from its serialised JSON payload.

        Used by ``__reduce__`` so pickle can round-trip the exception.
        """
        data = json.loads(payload)
        instance = cls.__new__(cls)
        instance.deployment_id = data.get("deployment_id", "")
        instance.logs_dict = data.get("logs")
        instance.tf_state = data.get("tf_state")
        instance.commit_info = data.get("commit_info")
        instance.terraform_outputs = data.get("terraform_outputs")
        Exception.__init__(instance, payload)
        return instance

    def __reduce__(self):
        # The single-arg constructor here is ``_from_payload``; args[0] is
        # the JSON string we built in __init__.
        return (Failure._from_payload, (self.args[0] if self.args else "{}",))

    def __repr__(self) -> str:
        # Pin the repr format that the backend's celery event listener
        # relies on (regex ``Failure\('(.+)'\)``).
        return f"Failure({self.args[0]!r})" if self.args else "Failure()"

    def to_dict(self) -> dict[str, Any]:
        """Convert exception data to dict for serialization"""
        return json.loads(str(self))


# --- Variable encoding for Packer/Terraform CLI ----------------------------


def _looks_like_file_var_value(value: Any) -> bool:
    """True if ``value`` matches the file-upload shape produced by
    the backend's ``_attach_files_to_user_input``: a non-empty
    mapping whose entries each carry a ``content_b64`` field plus
    the metadata triplet (name, size, content_type) — i.e. exactly
    the ``map(object(...))`` HCL contract.

    Used by :func:`_strip_file_vars` so destroy / cleanup-after-
    failure can drop ``@openstack:file:*``-marked variables before
    passing the var-set to ``terraform destroy``. Terraform
    validates *all* declared variables on every command — including
    destroy — so an apply-only file-var would otherwise block the
    cleanup with a schema error.

    A slot must carry ``content_b64`` to qualify as a file-var; the
    strictness avoids dropping legitimate non-file map variables that
    happen to share the metadata keys.
    """
    if not isinstance(value, dict) or not value:
        return False
    for slot in value.values():
        if not isinstance(slot, dict):
            return False
        if "content_b64" not in slot:
            return False
    return True


def _strip_file_vars(terraform_vars: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``terraform_vars`` with file-shape entries removed.

    Pure function — never mutates the input. Used by destroy and the
    deploy cleanup-after-failure branches; deploy itself keeps the
    file vars because ``apply`` consumes them via cloud-init.
    """
    return {k: v for k, v in terraform_vars.items() if not _looks_like_file_var_value(v)}


def _scrub_nested_nones(value: Any) -> Any:
    """Recursively drop ``None`` entries from nested dicts/lists.

    A stray ``None`` inside a ``map(list(string))`` slot would surface as
    literal HCL ``null`` after the JSON round-trip and trip Terraform's
    type check. Dicts have their ``None``-valued keys removed, lists have
    their ``None`` entries filtered out, and both are walked recursively.
    Scalars (including bools) pass through untouched.
    """
    if isinstance(value, dict):
        cleaned: dict[Any, Any] = {}
        for k, v in value.items():
            if v is None:
                continue
            cleaned[k] = _scrub_nested_nones(v)
        return cleaned
    if isinstance(value, list):
        return [_scrub_nested_nones(item) for item in value if item is not None]
    return value


def encode_terraform_vars(d: dict[str, Any]) -> dict[str, str]:
    """Encode variables for ``terraform -var key=value`` CLI args.

    Terraform reads complex types (objects, tuples) when the value is a
    valid JSON literal. We JSON-encode dicts/lists once and pass them
    through verbatim — no string normalisation that could damage escape
    sequences.

    Nested ``None`` values are scrubbed recursively (see
    :func:`_scrub_nested_nones`) so a stray ``null`` deep inside a
    ``map(list(string))`` slot can't trip Terraform's type check.
    """
    result: dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, bool):
            # HCL accepts lowercase only; ``str(True)`` would emit "True".
            result[k] = "true" if v else "false"
        elif isinstance(v, dict | list):
            result[k] = json.dumps(_scrub_nested_nones(v), ensure_ascii=False)
        else:
            result[k] = str(v)
    return result


def encode_packer_vars(d: dict[str, Any]) -> dict[str, str]:
    """Encode variables for ``packer -var key=value`` CLI args.

    For HCL ``list(...)``-typed variables, we emit a JSON array literal
    (e.g. ``["NAT"]``) — that's the only form Packer accepts via ``-var``
    for typed-list variables, since Packer parses each ``-var`` value as
    an HCL expression against the declared type. JSON arrays are valid
    HCL list literals, so a single representation covers both syntaxes.
    String values are passed through verbatim.
    """
    result: dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, list | dict):
            # JSON array works for ``list(string)``, ``list(number)`` etc.
            # ``map(...)``-typed vars take the same JSON literal path — no
            # Packer template in the project uses one today, but the
            # encoding is correct for when one shows up.
            # ``ensure_ascii=False`` lets non-ASCII names pass through
            # unchanged (Packer's HCL parser is UTF-8 native).
            result[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, bool):
            result[k] = "true" if v else "false"
        else:
            result[k] = str(v)
    return result


# --- Phase tracking ----------------------------------------------------------
#
# Phases are pinned by name (a string the frontend renders as a stepper) and
# by index (1-based, used for the percent bar). The list is split in two so
# the worker can collapse the Packer block when a deployment doesn't need a
# Packer build — that decision is made after the git clone has finished and
# we can see whether ``packer/template.pkr.hcl`` exists.

PHASE_STARTING = "STARTING"
PHASE_OPENSTACK_SETUP = "OPENSTACK_SETUP"
PHASE_GIT_CLONE = "GIT_CLONE"
PHASE_CREDS_MATERIALISE = "CREDS_MATERIALISE"
PHASE_PACKER_INIT = "PACKER_INIT"
PHASE_PACKER_VALIDATE = "PACKER_VALIDATE"
PHASE_PACKER_BUILD = "PACKER_BUILD"
PHASE_TERRAFORM_INIT = "TERRAFORM_INIT"
PHASE_TERRAFORM_PLAN = "TERRAFORM_PLAN"
PHASE_TERRAFORM_APPLY = "TERRAFORM_APPLY"
PHASE_OUTPUTS_AND_CLEANUP = "OUTPUTS_AND_CLEANUP"
PHASE_TERRAFORM_DESTROY = "TERRAFORM_DESTROY"
PHASE_CLEANUP = "CLEANUP"
# Pause/resume share the deploy/destroy preamble but their hot phase is
# a CLI-driven server stop/start, so they get distinct phase names rather
# than reusing TERRAFORM_DESTROY.
PHASE_SERVER_STOP = "SERVER_STOP"
PHASE_SERVER_START = "SERVER_START"

# Every pipeline opens with the same four phases and, after any Packer
# work, continues into terraform. Spelling the segments once keeps the six
# tuples below from drifting apart.
_PRE_PACKER_PHASES = (
    PHASE_STARTING,
    PHASE_OPENSTACK_SETUP,
    PHASE_GIT_CLONE,
    PHASE_CREDS_MATERIALISE,
)
_PACKER_PHASES = (
    PHASE_PACKER_INIT,
    PHASE_PACKER_VALIDATE,
    PHASE_PACKER_BUILD,
)
_POST_PACKER_PHASES = (
    PHASE_TERRAFORM_INIT,
    PHASE_TERRAFORM_PLAN,
    PHASE_TERRAFORM_APPLY,
    PHASE_OUTPUTS_AND_CLEANUP,
)
_PHASES_WITH_PACKER = _PRE_PACKER_PHASES + _PACKER_PHASES + _POST_PACKER_PHASES
_PHASES_WITHOUT_PACKER = _PRE_PACKER_PHASES + _POST_PACKER_PHASES


def _is_legacy_layout(templates: list[_PackerTemplate]) -> bool:
    """True for the flat ``packer/template.pkr.hcl`` layout (or no Packer).

    The legacy layout is what ``packer_discovery`` reports as a single
    template keyed ``"default"``. It differs from the multi-template layout
    in image naming, phase naming, the Packer working directory and the
    Packer/Terraform variable shape, so the test is spelled once here
    rather than at each of those decisions.
    """
    return not templates or (len(templates) == 1 and templates[0].key == "default")


def _phases_for_plans(plans: list["_ImagePlan"]) -> tuple[str, ...]:
    """Build the phase tuple for a deployment's image plans.

    * No plans → ``_PHASES_WITHOUT_PACKER`` (clone, then straight to
      terraform).
    * Legacy single-image layout → ``_PHASES_WITH_PACKER`` verbatim, with
      the Packer phases unsuffixed.
    * Multi-image → one ``PACKER_INIT:<key>`` / ``PACKER_VALIDATE:<key>``
      / ``PACKER_BUILD:<key>`` trio per plan, in the order discovery
      returned them (sorted by key, so the stepper order is
      deterministic).

    The phase names come off the plan, so this function and
    ``_build_one_packer_image`` cannot disagree about what a phase is
    called — the UI matches them by string.
    """
    if not plans:
        return _PHASES_WITHOUT_PACKER
    if plans[0].is_legacy:
        return _PHASES_WITH_PACKER
    packer_phases = tuple(name for plan in plans for name in plan.phase_names)
    return _PRE_PACKER_PHASES + packer_phases + _POST_PACKER_PHASES


# Destroy runs a shorter pipeline: no Packer (no fresh image is needed to
# tear things down) and no separate plan phase (terraform destroy plans
# internally and we don't surface that as its own progress step).
_PHASES_DESTROY = _PRE_PACKER_PHASES + (PHASE_TERRAFORM_INIT, PHASE_TERRAFORM_DESTROY, PHASE_CLEANUP)
# Per-VM redeploy reuses the destroy preamble and then runs
# ``terraform apply -replace=<addr> -target=<addr>`` instead of destroy.
_PHASES_REDEPLOY = _PRE_PACKER_PHASES + (PHASE_TERRAFORM_INIT, PHASE_TERRAFORM_APPLY, PHASE_CLEANUP)
# Pause / resume share that preamble too — terraform init is what lets
# them pull the canonical state from the pg backend. Their hot phase is a
# CLI-driven server stop/start; CLEANUP mirrors destroy's tail.
_PHASES_PAUSE = _PRE_PACKER_PHASES + (PHASE_TERRAFORM_INIT, PHASE_SERVER_STOP, PHASE_CLEANUP)
_PHASES_RESUME = _PRE_PACKER_PHASES + (PHASE_TERRAFORM_INIT, PHASE_SERVER_START, PHASE_CLEANUP)


class _PhaseTracker:
    """Drives ``StructuredLogger.progress`` calls.

    The set of phases is fixed at construction time so the percent bar
    monotonically advances; ``mark()`` looks up the index of the named
    phase and sends a progress event with the correct ``idx/total``.
    """

    def __init__(self, logger: Any, phases: tuple[str, ...]):
        self._logger = logger
        self._phases = phases
        self._index_by_name = {name: i for i, name in enumerate(phases, start=1)}

    @property
    def total(self) -> int:
        return len(self._phases)

    def mark(self, phase_name: str, message: str = "") -> None:
        idx = self._index_by_name.get(phase_name)
        if idx is None:
            # Unknown phase — emit a transcript marker but no progress
            # update so the bar doesn't reset.
            self._logger.phase(phase_name)
            return
        # Buffer the readable phase header and emit the live progress event.
        # The full phase-name sequence rides on every event so the UI can
        # render each stepper slot with its real label immediately.
        self._logger.phase(phase_name)
        self._logger.progress(
            phase_name,
            idx,
            self.total,
            message,
            phase_names=self._phases,
        )


def _terraform_executor(terraform_dir, openstack_env, tfstate_conn_str, tfstate_schema) -> TerraformExecutor:
    """Build a TerraformExecutor bound to the deployment's pg-backend schema."""
    return TerraformExecutor(
        terraform_dir,
        env_vars=openstack_env,
        backend_conn_str=tfstate_conn_str,
        backend_schema_name=tfstate_schema,
    )


def collect_terraform_state_helper(
    terraform_dir, openstack_env, tfstate_conn_str, tfstate_schema, task_logger, *, local_fallback=False
):
    """Snapshot the terraform state for the task row (best-effort).

    With the pg backend the canonical state lives in Postgres; this
    snapshot is used for debugging only. When ``local_fallback`` is set
    (deploy path), a missing/empty pull falls back to reading the local
    ``terraform.tfstate`` file for legacy/test modes that don't configure
    a remote backend. Returns ``None`` when nothing could be read.
    """
    if not (terraform_dir and os.path.exists(terraform_dir)):
        return None
    try:
        pulled = _terraform_executor(terraform_dir, openstack_env, tfstate_conn_str, tfstate_schema).state_pull()
        if pulled or not local_fallback:
            return pulled
    except Exception as e:
        task_logger.warning(f"Could not pull terraform state: {e}", category=LogCategory.WARNING)
        if not local_fallback:
            return None

    # Legacy fallback — only relevant when no pg backend is configured.
    tfstate_path = os.path.join(terraform_dir, "terraform.tfstate")
    if os.path.exists(tfstate_path):
        try:
            with open(tfstate_path) as f:
                return f.read()
        except Exception as e:
            task_logger.warning(f"Could not read terraform state: {e}", category=LogCategory.WARNING)
    return None


def collect_terraform_outputs_helper(terraform_dir, openstack_env, tfstate_conn_str, tfstate_schema, task_logger):
    """Collect terraform outputs even on partial success. ``None`` on failure."""
    if terraform_dir and os.path.exists(terraform_dir):
        try:
            return _terraform_executor(terraform_dir, openstack_env, tfstate_conn_str, tfstate_schema).output()
        except Exception as e:
            task_logger.warning(f"Could not read terraform outputs: {e}", category=LogCategory.WARNING)
    return None


def _extract_commit_info(repo_path: str) -> dict[str, Any]:
    """Read the checked-out commit's metadata from a cloned repo.

    Returns the dict shape persisted into the task result and the
    ``Failure`` payload. Callers wrap this in their own try/except so a
    repo without a readable HEAD degrades to a warning, not a hard fail.
    """
    repo = git.Repo(repo_path)
    commit = repo.head.commit
    return {
        "hash": commit.hexsha,
        "message": commit.message.strip(),
        "author": str(commit.author),
        "date": commit.committed_datetime.isoformat(),
    }


def _image_tag(commit_info: dict[str, Any] | None, release: str) -> str:
    """Cache key for the built image: the commit SHA, falling back to the tag.

    ``release`` is often a moving ref (e.g. "main"), so the content-addressed
    short SHA is what makes a new commit miss the image cache.
    """
    return commit_info["hash"][:8] if commit_info and commit_info.get("hash") else release


@dataclass(frozen=True)
class _ImagePlan:
    """Every per-template decision, resolved once from the discovered layout.

    ``packer_discovery`` is the only place that knows whether the app repo
    used the flat ``packer/template.pkr.hcl`` layout or the multi-template
    ``packer/<key>/`` one. Before this type, that single fact was re-derived
    at eight call sites to answer eight different questions — image name,
    Terraform variable name, phase names, Packer working directory, Packer
    variable nesting and log prefix — and the copies had already drifted
    apart. Now the layout is interpreted once, in :func:`_plan_images`, and
    everything downstream reads a resolved answer off the plan.

    Deploy uses a plan to *build* an image; destroy and redeploy build the
    same plans to *name* the images the original deploy produced, so the
    Terraform variables still validate against the pg-backend state.
    """

    key: str
    is_legacy: bool

    @property
    def log_prefix(self) -> str:
        """Per-template log prefix; empty for the legacy single-image layout."""
        return "" if self.is_legacy else f"[{self.key}] "

    @property
    def terraform_var_name(self) -> str:
        """HCL variable the image name is passed in as."""
        return "image_name" if self.is_legacy else f"image_name_{self.key}"

    @property
    def phase_names(self) -> tuple[str, str, str]:
        """The (init, validate, build) phase names for this template."""
        suffix = "" if self.is_legacy else f":{self.key}"
        return (
            f"{PHASE_PACKER_INIT}{suffix}",
            f"{PHASE_PACKER_VALIDATE}{suffix}",
            f"{PHASE_PACKER_BUILD}{suffix}",
        )

    def image_name(self, app_id: str, image_tag: str) -> str:
        """Glance image name. Must match byte-for-byte across deploy/destroy/redeploy."""
        return f"{app_id}-{image_tag}" if self.is_legacy else f"{app_id}-{self.key}-{image_tag}"

    def packer_dir(self, repo_path: str) -> str:
        """Working directory for ``packer init/validate/build``."""
        if self.is_legacy:
            return os.path.join(repo_path, "packer")
        return os.path.join(repo_path, "packer", self.key)

    def user_packer_vars(self, user_vars: dict[str, Any]) -> dict[str, Any]:
        """This template's slice of the user-supplied Packer variables.

        Legacy apps use a flat ``user_vars["packer"][var]``; multi-image
        apps nest one level deeper under the template key.
        """
        if self.is_legacy:
            return {**user_vars.get("packer", {})}
        return {**((user_vars.get("packer") or {}).get(self.key, {}) or {})}


def _plan_images(templates: list[_PackerTemplate], *, legacy_fallback: bool = False) -> list[_ImagePlan]:
    """Normalise the discovered templates into one plan per image.

    The single place the legacy/multi distinction is interpreted.

    ``legacy_fallback`` reproduces a pre-existing asymmetry between the
    tasks, deliberately: with no Packer template at all, deploy injects no
    image variable, while destroy and redeploy inject a flat
    ``image_name=<app_id>-<tag>``. Only destroy and redeploy pass
    ``legacy_fallback=True``. The two behaviours are not obviously both
    right — an app with no Packer declares no ``image_name``, and
    ``terraform -var`` on an undeclared variable is an error — but
    changing either is a behaviour change, not a refactor, so the split is
    preserved and named here rather than left implicit in two spellings of
    a predicate.
    """
    if not templates and legacy_fallback:
        return [_ImagePlan(key="default", is_legacy=True)]
    legacy = _is_legacy_layout(templates)
    return [_ImagePlan(key=t.key, is_legacy=legacy) for t in templates]


def _apply_image_name_vars(target: dict[str, Any], plans: list[_ImagePlan], app_id: str, image_tag: str) -> None:
    """Inject each plan's image-name variable into a Terraform var-set."""
    for plan in plans:
        target[plan.terraform_var_name] = plan.image_name(app_id, image_tag)


def _require_ok(
    result: tuple[bool, str, str],
    *,
    op: str,
    task_logger: Any,
    error_message: str,
    log_error: bool = False,
) -> str:
    """Return a CLI step's stdout, or log its output and raise.

    The executors log richly, but to a module-level ``StructuredLogger``
    that has no event emitter and whose buffer nobody drains — so none of
    it reaches the per-deployment transcript the frontend renders. Every
    call site therefore re-logged the tool's own output by hand before
    raising. This is that block, written once.

    Both streams are logged because ``_stream_subprocess`` merges stderr
    into stdout for streamed commands but returns a real stderr on a
    timeout or an internal failure.
    """
    success, stdout, stderr = result
    if success:
        return stdout
    for stream_name, text in (("stdout", stdout), ("stderr", stderr)):
        if text:
            task_logger.command_output(f"{op}_{stream_name}", text, returncode=1)
    # Only deploy's terraform steps emit a separate ERROR entry on top of
    # the command output; the other call sites never did, and the
    # transcript is user-visible, so the difference is preserved.
    if log_error:
        task_logger.error(error_message, category=LogCategory.ERROR)
    raise Exception(error_message)


@dataclass
class _TaskContext:
    """Everything the four lifecycle tasks used to set up by hand.

    Before this type, each task opened with ~25 lines of identical
    scaffolding — logger, event emitter, phase tracker, the pg-backend
    coordinates, four ``None`` locals and two collector closures — and
    closed with an identical ``finally``. Four copies that had already
    drifted apart in small ways.

    The mutable fields (``repo_path``, ``openstack_env``,
    ``terraform_dir``) are filled in as the task progresses, because the
    failure path needs whatever was reached before it failed.
    """

    deployment_id: str
    task_logger: Any
    phase_tracker: "_PhaseTracker"
    tfstate_conn_str: str | None
    tfstate_schema: str
    stack: contextlib.ExitStack

    repo_path: str | None = None
    openstack_env: dict[str, str] = field(default_factory=dict)
    terraform_dir: str | None = None
    commit_info: dict[str, Any] | None = None

    def set_phases(self, phases: tuple[str, ...]) -> None:
        """Re-plan the phase list once the real pipeline shape is known.

        Deploy starts pessimistic (assume a Packer build) and narrows this
        after the clone, so the percent bar reflects the phases that will
        actually run.
        """
        self.phase_tracker = _PhaseTracker(self.task_logger, phases)

    def mark(self, phase_name: str, message: str = "") -> None:
        """Advance the progress bar. Shorthand for ``phase_tracker.mark``."""
        self.phase_tracker.mark(phase_name, message)

    def stream_line(self, tool: str, line: str) -> None:
        """Feed one line of subprocess output into the per-deployment log."""
        self.task_logger.tool_output_line(tool, line)

    def terraform(self, *, streamed: bool = True) -> TerraformExecutor:
        """A TerraformExecutor bound to this deployment's pg-backend schema."""
        return TerraformExecutor(
            self.terraform_dir,
            env_vars=self.openstack_env,
            backend_conn_str=self.tfstate_conn_str,
            backend_schema_name=self.tfstate_schema,
            output_callback=self.stream_line if streamed else None,
        )

    def collect_state(self, *, local_fallback: bool = False) -> str | None:
        """Best-effort terraform state snapshot for the task row."""
        return collect_terraform_state_helper(
            self.terraform_dir,
            self.openstack_env,
            self.tfstate_conn_str,
            self.tfstate_schema,
            self.task_logger,
            local_fallback=local_fallback,
        )

    def collect_outputs(self) -> dict[str, Any] | None:
        """Best-effort terraform outputs, even on partial success."""
        return collect_terraform_outputs_helper(
            self.terraform_dir,
            self.openstack_env,
            self.tfstate_conn_str,
            self.tfstate_schema,
            self.task_logger,
        )

    def require_terraform_dir(self) -> str:
        """Locate ``terraform/`` inside the clone and remember it."""
        terraform_dir = os.path.join(self.repo_path, "terraform")
        if not os.path.exists(terraform_dir):
            raise Exception(f"Terraform directory not found at {terraform_dir}")
        self.terraform_dir = terraform_dir
        return terraform_dir

    def result(
        self,
        *,
        tf_state: str | None = None,
        outputs: dict[str, Any] | None = None,
        commit_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """The success payload. Shape is fixed by the backend's event listener."""
        return {
            "status": "success",
            "deployment_id": self.deployment_id,
            "logs": self.task_logger.get_logs_dict(),
            "tf_state": tf_state,
            "commit_info": commit_info,
            # Passed through verbatim: deploy reports ``None`` when the
            # output collection itself failed, which is distinguishable
            # from "applied but declared no outputs" ({}). The backend
            # persists the difference.
            "terraform_outputs": outputs,
        }


@contextlib.contextmanager
def _task_context(bound_task: Any, deployment_id: str, verb: str, phases: tuple[str, ...]):
    """Set up and tear down one lifecycle task.

    Wires the per-deployment logger to Celery's event bus, builds the
    phase tracker, and owns an ``ExitStack`` for everything the task
    acquires. Teardown runs LIFO, so registering the repo cleanup before
    entering ``PerTaskCloudsConfig`` shreds the credential file first and
    removes the clone second — the order the old hand-written ``finally``
    spelled out with a comment explaining why.
    """
    task_logger = get_logger(f"{verb}:{deployment_id}", correlation_id=deployment_id)

    def _emit(event_name: str, payload: dict[str, Any]) -> None:
        # ``deployment_id`` rides on every event so the backend listener
        # doesn't need a DB lookup to route it.
        bound_task.send_event(event_name, deployment_id=deployment_id, **payload)

    task_logger.set_event_emitter(_emit)

    with contextlib.ExitStack() as stack:
        yield _TaskContext(
            deployment_id=deployment_id,
            task_logger=task_logger,
            phase_tracker=_PhaseTracker(task_logger, phases),
            tfstate_conn_str=settings.TFSTATE_DATABASE_URL or None,
            tfstate_schema=_tfstate_schema_name(deployment_id),
            stack=stack,
        )


def _task_preamble(
    ctx: _TaskContext,
    *,
    app_git_link: str,
    release: str,
    openstack_envelope: dict[str, Any] | None,
    envelope_error: str,
    clone_message: str,
    clone_detail: str | None = None,
    commit_info_mode: str = "full",
    frame_creds_operation: bool = False,
) -> None:
    """The four opening phases every lifecycle task shares.

    Validates the credential envelope, clones the app repo at the release
    tag, reads the checked-out commit, and materialises the per-task
    clouds.yaml inside the clone. Fills ``ctx.repo_path``,
    ``ctx.openstack_env`` and ``ctx.commit_info``.

    Cleanup for both the clone and the credential file is registered on
    ``ctx.stack`` as it is acquired, so a failure half-way through still
    tears down exactly what was created.

    The message parameters exist because the four tasks word these phases
    differently and the transcript is user-visible — this helper unifies
    the control flow, not the wording. ``commit_info_mode`` is
    ``"full"`` (resource_info + success line, as deploy and destroy do),
    ``"success_only"`` (redeploy) or ``"none"`` (pause/resume, which never
    look at the commit).
    """
    ctx.mark(PHASE_OPENSTACK_SETUP, "Validating OpenStack credentials")
    if not openstack_envelope:
        raise Exception(envelope_error)
    ctx.task_logger.success("OpenStack credential envelope received", category=LogCategory.STATUS)

    ctx.mark(PHASE_GIT_CLONE, clone_message)
    if clone_detail:
        ctx.task_logger.info(clone_detail, category=LogCategory.OPERATION)
    try:
        ctx.repo_path = git_service.clone_release(git_url=app_git_link, deployment_id=ctx.deployment_id, tag=release)
    except Exception as e:
        raise Exception(f"Git clone failed: {str(e)}")
    ctx.stack.callback(_cleanup_repository, ctx.repo_path, ctx.task_logger)

    if commit_info_mode == "none":
        ctx.task_logger.success("Repository cloned", category=LogCategory.STATUS)
    else:
        # A repo without a readable HEAD degrades to a warning: the commit
        # metadata is for display and image-cache keying, not correctness.
        try:
            ctx.commit_info = _extract_commit_info(ctx.repo_path)
            if commit_info_mode == "full":
                ctx.task_logger.resource_info(
                    "git_commit",
                    ctx.commit_info["hash"][:8],
                    hash=ctx.commit_info["hash"],
                    message=ctx.commit_info["message"],
                    author=ctx.commit_info["author"],
                )
            ctx.task_logger.success(
                f"Repository cloned at commit {ctx.commit_info['hash'][:8]}",
                category=LogCategory.STATUS,
            )
        except Exception as e:
            ctx.task_logger.warning(f"Could not extract commit info: {e}", category=LogCategory.WARNING)

    ctx.mark(PHASE_CREDS_MATERIALISE, "Writing per-task clouds.yaml")
    if frame_creds_operation:
        ctx.task_logger.operation_start("openstack_credentials_materialise")
    # Registered after the repo cleanup, so LIFO shreds the credential
    # file first — even if removing the clone then fails or hangs.
    ctx.openstack_env = ctx.stack.enter_context(
        _shredding_clouds_config(openstack_envelope, ctx.repo_path, ctx.task_logger)
    )
    if frame_creds_operation:
        ctx.task_logger.operation_end("openstack_credentials_materialise", success=True)
    ctx.task_logger.success("Per-task clouds.yaml written", category=LogCategory.STATUS)


@contextlib.contextmanager
def _shredding_clouds_config(envelope: dict[str, Any], work_dir: str, task_logger: Any):
    """PerTaskCloudsConfig whose teardown failure is a warning, not an error.

    Cleanup must never mask the task's real result or exception.
    """
    config = PerTaskCloudsConfig(envelope, work_dir=work_dir)
    env = config.__enter__()
    try:
        yield env
    finally:
        try:
            config.__exit__(None, None, None)
        except Exception as e:
            task_logger.warning(f"Per-task clouds.yaml cleanup failed: {e}", category=LogCategory.WARNING)


def _cleanup_repository(repo_path: str | None, task_logger: Any) -> None:
    """Remove the cloned repo, downgrading any failure to a warning."""
    if not repo_path:
        return
    try:
        git_service.cleanup_repository(repo_path)
        task_logger.success("Repository cleanup completed", category=LogCategory.SYSTEM)
    except Exception as e:
        task_logger.warning(f"Repository cleanup failed: {e}", category=LogCategory.WARNING)


def _terraform_var_set(
    raw: dict[str, Any],
    *,
    plans: list[_ImagePlan],
    app_id: str,
    image_tag: str,
    teams: dict[str, list],
    strip_files: bool = False,
    transform: Any = None,
) -> dict[str, str]:
    """Assemble and encode the Terraform variable set for one run.

    One recipe for every path. Deploy's happy path and its
    cleanup-after-failure path used to build this separately, differing
    only by ``strip_files`` — so the two could silently drift on
    everything else.

    ``strip_files`` drops ``content_b64``-shaped upload variables. Those
    are consumed at apply time via cloud-init; destroy doesn't need the
    bytes, but Terraform validates every declared variable on every
    command, so an apply-only file var would otherwise reject the run.

    ``transform`` is an optional hook applied to the raw var-set before
    the image names go in — redeploy uses it to reconcile scoped
    variables against the current roster.

    ``raw`` is passed in rather than extracted from ``user_vars`` here
    because the call sites do not agree on how to extract it, and the
    difference is observable for a present-but-``None`` value.
    """
    if strip_files:
        raw = _strip_file_vars(raw)
    if transform is not None:
        raw = transform(raw)
    _apply_image_name_vars(raw, plans, app_id, image_tag)
    if teams:
        raw["users"] = teams
    return encode_terraform_vars(raw)


def _discover_image_plans(
    ctx: _TaskContext, app_id: str, release: str, *, legacy_fallback: bool = False
) -> tuple[list[_ImagePlan], str]:
    """Discover the packer layout and resolve it into image plans.

    Deploy uses the result to build the images; destroy and redeploy use
    it to name the same images the original deploy produced, so the
    Terraform variables still validate against the pg-backend state.
    """
    try:
        templates = _discover_packer_templates(ctx.repo_path)
    except PackerTemplateDiscoveryError as e:
        raise Exception(f"Packer template discovery failed: {e}")
    return _plan_images(templates, legacy_fallback=legacy_fallback), _image_tag(ctx.commit_info, release)


def _raise_failure(
    ctx: _TaskContext,
    error: Exception,
    *,
    verb: str,
    tf_state: str | None = None,
    outputs: dict[str, Any] | None = None,
    commit_info: dict[str, Any] | None = None,
    collect_state: bool = True,
    local_fallback: bool = False,
) -> NoReturn:
    """Log the failure and re-raise it as the ``Failure`` the backend parses.

    Collects whatever state and outputs are still reachable first — a
    half-applied deployment usually has both, and they are what makes the
    failure debuggable in the UI.
    """
    ctx.task_logger.exception(f"{verb} failed: {str(error)}", exception=error, deployment_id=ctx.deployment_id)
    if not tf_state and collect_state:
        tf_state = ctx.collect_state(local_fallback=local_fallback)
    raise Failure(
        message=str(error),
        deployment_id=ctx.deployment_id,
        logs_dict=ctx.task_logger.get_logs_dict(),
        tf_state=tf_state,
        commit_info=commit_info,
        terraform_outputs=outputs,
    )


def _build_one_packer_image(
    plan: _ImagePlan,
    *,
    image_name,
    openstack_service,
    project_id,
    repo_path,
    openstack_env,
    stream_line,
    user_vars,
    phase_tracker,
    task_logger,
):
    """Build (or reuse) the Packer image for a single template.

    Skips the build when the image already exists in Glance, and
    coordinates concurrent workers via ``PackerBuildLock`` (only one
    worker builds a given image; the others wait and reuse it). Raises
    ``Exception("Packer error: ...")`` on any failure.
    """
    # Every legacy-vs-multi decision is already resolved on the plan.
    log_prefix = plan.log_prefix
    init_phase, validate_phase, build_phase = plan.phase_names

    wait_announced = False
    # PackerBuildLock is a context manager; ``__exit__`` releases the lock
    # and stops the TTL heartbeat thread, which is exactly what the old
    # hand-rolled try/finally did.
    try:
        with PackerBuildLock(project_id, image_name) as build_lock:
            while True:
                # If the image already exists, skip the build and the lock.
                exists, image_id = openstack_service.check_image_exists(image_name)
                if exists:
                    task_logger.success(
                        f"{log_prefix}Image '{image_name}' already exists (ID: {image_id}). Skipping Packer build.",
                        category=LogCategory.STATUS,
                    )
                    break

                held = build_lock.acquire_or_wait()
                if not held:
                    # Another worker is still building the same image. Surface
                    # this in the per-deployment log once so the frontend's
                    # live tail shows *something* during the 5-second poll
                    # cycles — without it the browser sees no events and looks
                    # frozen.
                    if not wait_announced:
                        task_logger.info(
                            f"{log_prefix}Another worker is currently building image '{image_name}'. Waiting…",
                            category=LogCategory.STATUS,
                        )
                        wait_announced = True
                    # We slept inside acquire_or_wait; re-check Glance.
                    continue

                # Re-check after acquiring: another worker may have finished its
                # build between our last check and our lock acquisition.
                exists, image_id = openstack_service.check_image_exists(image_name)
                if exists:
                    task_logger.success(
                        f"{log_prefix}Image '{image_name}' built by another worker (ID: {image_id}). Skipping.",
                        category=LogCategory.STATUS,
                    )
                    break

                task_logger.info(
                    f"{log_prefix}Image '{image_name}' does not exist. Building...",
                    category=LogCategory.OPERATION,
                )

                # Pick the right packer working directory: legacy uses
                # ``packer/`` directly; multi uses ``packer/<key>/``. Template
                # file name is always ``template.pkr.hcl`` relative to that
                # directory.
                packer_dir = plan.packer_dir(repo_path)
                packer = PackerExecutor(
                    packer_dir,
                    env_vars=openstack_env,
                    output_callback=stream_line,
                )

                # Per-template Packer variables. Legacy shape is the flat
                # ``user_vars["packer"][var_name]``; multi shape is nested
                # ``user_vars["packer"][template_key][var_name]``.
                packer_vars = plan.user_packer_vars(user_vars)
                packer_vars["image_name"] = image_name
                packer_vars = encode_packer_vars(packer_vars)

                task_logger.info(
                    f"{log_prefix}Packer variable keys",
                    category=LogCategory.OPERATION,
                    keys=list(packer_vars.keys()),
                    template=plan.key,
                    image_name=image_name,
                )

                phase_tracker.mark(init_phase, f"{log_prefix}Initializing Packer plugins")
                _require_ok(
                    packer.init(),
                    op="packer_init",
                    task_logger=task_logger,
                    error_message=f"{log_prefix}Packer init failed",
                )

                phase_tracker.mark(validate_phase, f"{log_prefix}Validating Packer template")
                success, stdout, stderr = packer.validate("template.pkr.hcl", packer_vars)
                if not success:
                    raise Exception(f"{log_prefix}Packer validation failed: {stderr}")

                phase_tracker.mark(
                    build_phase,
                    f"{log_prefix}Building image '{image_name}' (this may take minutes)",
                )
                success, output = packer.build("template.pkr.hcl", packer_vars)
                if not success:
                    raise Exception(f"{log_prefix}Packer build failed: {output}")

                task_logger.success(
                    f"{log_prefix}Image '{image_name}' built successfully",
                    category=LogCategory.STATUS,
                )
                break
    except Exception as e:
        raise Exception(f"Packer error: {str(e)}")


def _packer_step(
    ctx: _TaskContext,
    plans: list[_ImagePlan],
    *,
    app_id: str,
    image_tag: str,
    openstack_envelope: dict[str, Any],
    user_vars: dict[str, Any],
) -> None:
    """Build (or reuse) every image this deployment needs.

    Each plan gets its own Redis lock and image-exists check keyed on
    (project, image name), so two workers can't both kick off a build for
    the same image — and two workers *can* build different images of the
    same app in parallel.
    """
    if not plans:
        ctx.task_logger.info("No Packer template found, skipping image build", category=LogCategory.SYSTEM)
        return

    project_id = openstack_envelope.get("project_id") or openstack_envelope.get("project_name") or "default"
    openstack_service = OpenStackService(env_vars=ctx.openstack_env)
    for plan in plans:
        _build_one_packer_image(
            plan,
            image_name=plan.image_name(app_id, image_tag),
            openstack_service=openstack_service,
            project_id=project_id,
            repo_path=ctx.repo_path,
            openstack_env=ctx.openstack_env,
            stream_line=ctx.stream_line,
            user_vars=user_vars,
            phase_tracker=ctx.phase_tracker,
            task_logger=ctx.task_logger,
        )


# A file-upload variable can balloon a single -var to hundreds of KB. The
# Nova metadata service caps cloud-init user_data at ~64 KB compressed, so
# anything approaching that is worth a heads-up in the log before the VM
# fails to boot for reasons the user can't see.
_VAR_SIZE_WARN_BYTES = 120 * 1024


def _warn_oversized_vars(ctx: _TaskContext, terraform_vars: dict[str, str]) -> None:
    """Warn per variable whose encoded form approaches the cloud-init limit."""
    for name, value in terraform_vars.items():
        if isinstance(value, str) and len(value) > _VAR_SIZE_WARN_BYTES:
            ctx.task_logger.warning(
                f"Terraform variable '{name}' is {len(value) // 1024} KB encoded — close to the "
                "cloud-init user_data limit; the VM may fail to boot if the template "
                "inlines the full value.",
                category=LogCategory.WARNING,
            )


def _cleanup_partial_apply(
    ctx: _TaskContext,
    terraform: TerraformExecutor,
    *,
    user_vars: dict[str, Any],
    plans: list[_ImagePlan],
    app_id: str,
    image_tag: str,
    teams: dict[str, list],
) -> None:
    """Reverse a half-finished apply so it doesn't leak quota.

    A partial ``terraform apply`` typically leaves orphaned networks,
    ports and volumes that quietly eat the project's quota. Failures here
    are swallowed: we're already on the error path and about to re-raise
    the real cause.

    The var-set is rebuilt through the same recipe as the apply, minus the
    file payloads — destroy doesn't need the cloud-init bytes, but
    Terraform validates every declared variable on every command, so an
    apply-only file var would reject the cleanup outright.
    """
    try:
        ctx.task_logger.info(
            "Running terraform destroy to clean up partially-applied resources",
            category=LogCategory.OPERATION,
        )
        terraform.destroy(
            variables=_terraform_var_set(
                user_vars.get("terraform") or {},
                plans=plans,
                app_id=app_id,
                image_tag=image_tag,
                teams=teams,
                strip_files=True,
            )
        )
    except Exception as cleanup_error:
        ctx.task_logger.warning(f"Terraform cleanup failed: {cleanup_error}", category=LogCategory.WARNING)


@celery_app.task(bind=True, name="tasks.deploy_application")
def deploy_application(
    self,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] = None,
    openstack_envelope: dict[str, Any] | None = None,
):
    """Deploy an application: clone → (optional) Packer image → terraform apply.

    Args:
        deployment_id: UUID of the deployment.
        app_id: Application identifier; part of every built image's name.
        app_git_link: Git repo URL.
        release: Tag/release to check out.
        user_vars: User variables, split into ``packer`` and ``terraform``.
        teams: Teams with user emails, ``{"team": [{"email": "..."}]}``.
        openstack_envelope: Encrypted per-user OpenStack credential
            envelope from the backend. Required; the optional default
            exists only so older queued messages don't crash the worker on
            rollout, and we raise immediately if it's missing.

    Returns:
        dict: status, deployment_id, logs, tf_state, commit_info, terraform_outputs
    """
    if teams is None:
        teams = {}
    # Pessimistic phase set — assumes Packer. Narrowed once the clone
    # reveals whether the repo actually has a template.
    with _task_context(self, deployment_id, "deploy", _PHASES_WITH_PACKER) as ctx:
        tf_state: str | None = None
        outputs: dict[str, Any] | None = None
        try:
            ctx.mark(PHASE_STARTING, "Starting deployment")
            ctx.task_logger.resource_info(
                "deployment",
                deployment_id,
                app_id=app_id,
                git_url=app_git_link,
                release=release,
                user_vars_keys=list(user_vars.keys()),
                teams_keys=list(teams.keys()),
            )

            _task_preamble(
                ctx,
                app_git_link=app_git_link,
                release=release,
                openstack_envelope=openstack_envelope,
                envelope_error=(
                    "OpenStack credential envelope missing — user must upload credentials before deploying"
                ),
                clone_message="Cloning repository",
                clone_detail=f"Cloning repository: {app_git_link}",
                frame_creds_operation=True,
            )

            # The image is cached by commit SHA, not release tag: `release`
            # is often a moving ref (e.g. "main"), so the content-addressed
            # short SHA is what makes a new commit miss the cache.
            plans, image_tag = _discover_image_plans(ctx, app_id, release)
            # Now that we know whether this deployment needs a Packer build,
            # correct the phase total so the percent bar is honest. The next
            # progress event then lands on the right index.
            ctx.set_phases(_phases_for_plans(plans))

            _packer_step(
                ctx,
                plans,
                app_id=app_id,
                image_tag=image_tag,
                openstack_envelope=openstack_envelope,
                user_vars=user_vars,
            )

            ctx.require_terraform_dir()
            terraform = ctx.terraform()
            try:
                ctx.mark(PHASE_TERRAFORM_INIT, "Initializing Terraform")
                _require_ok(
                    terraform.init(),
                    op="terraform_init",
                    task_logger=ctx.task_logger,
                    error_message="Terraform init failed",
                    log_error=True,
                )
                ctx.task_logger.success("Terraform initialization completed", category=LogCategory.STATUS)

                # File vars are kept here: apply consumes them via cloud-init.
                terraform_vars = _terraform_var_set(
                    {**user_vars["terraform"]} if "terraform" in user_vars else {},
                    plans=plans,
                    app_id=app_id,
                    image_tag=image_tag,
                    teams=teams,
                )
                _warn_oversized_vars(ctx, terraform_vars)
                ctx.task_logger.info(
                    "Terraform variable keys",
                    category=LogCategory.OPERATION,
                    keys=list(terraform_vars.keys()),
                )

                ctx.mark(PHASE_TERRAFORM_PLAN, "Planning Terraform deployment")
                _require_ok(
                    terraform.plan(variables=terraform_vars),
                    op="terraform_plan",
                    task_logger=ctx.task_logger,
                    error_message="Terraform plan failed",
                    log_error=True,
                )
                ctx.task_logger.success("Terraform plan completed successfully", category=LogCategory.STATUS)

                ctx.mark(PHASE_TERRAFORM_APPLY, "Applying configuration (this may take minutes)")
                _require_ok(
                    terraform.apply(variables=terraform_vars),
                    op="terraform_apply",
                    task_logger=ctx.task_logger,
                    error_message="Terraform apply failed",
                    log_error=True,
                )
                ctx.task_logger.success("Terraform resources created", category=LogCategory.STATUS)

                ctx.mark(PHASE_OUTPUTS_AND_CLEANUP, "Collecting outputs")
                outputs = ctx.collect_outputs()
                tf_state = ctx.collect_state(local_fallback=True)
                if outputs:
                    ctx.task_logger.info(
                        "Terraform deployment outputs collected",
                        category=LogCategory.OPERATION,
                        output_count=len(outputs),
                    )

            except Exception as e:
                # Salvage whatever partial results exist, then reverse the
                # half-applied graph before re-raising the real cause.
                tf_state = ctx.collect_state(local_fallback=True)
                outputs = ctx.collect_outputs()
                if ctx.terraform_dir and os.path.exists(ctx.terraform_dir):
                    _cleanup_partial_apply(
                        ctx,
                        terraform,
                        user_vars=user_vars,
                        plans=plans,
                        app_id=app_id,
                        image_tag=image_tag,
                        teams=teams,
                    )
                    # Refresh so the persisted record reflects the cleanup.
                    tf_state = ctx.collect_state(local_fallback=True)
                raise Exception(f"Terraform error: {str(e)}")

            # The OUTPUTS_AND_CLEANUP progress event already fired above; a
            # second mark would land on the same index, so only log here.
            ctx.task_logger.success(f"Deployment {deployment_id} completed successfully", category=LogCategory.STATUS)
            ctx.task_logger.info("Deployment summary", category=LogCategory.SYSTEM, **ctx.task_logger.get_summary())
            if outputs:
                ctx.task_logger.info("Terraform deployment output", category=LogCategory.SYSTEM, **outputs)

            return ctx.result(tf_state=tf_state, outputs=outputs, commit_info=ctx.commit_info)

        except Exception as e:
            if not outputs:
                outputs = ctx.collect_outputs()
            _raise_failure(
                ctx,
                e,
                verb="Deployment",
                tf_state=tf_state,
                outputs=outputs,
                commit_info=ctx.commit_info,
                local_fallback=True,
            )


@celery_app.task(bind=True, name="tasks.destroy_deployment")
def destroy_deployment(
    self,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] = None,
    openstack_envelope: dict[str, Any] | None = None,
):
    """Tear down a deployment via ``terraform destroy``.

    Shares ``deploy_application``'s preamble (git clone at the same
    release tag, per-task clouds.yaml, same pg backend schema) so
    Terraform sees the exact state it built, then runs
    ``terraform destroy -auto-approve`` instead of ``plan + apply``. The
    Packer image is left in Glance so a future deploy of the same commit
    doesn't have to rebuild it.

    Args mirror ``deploy_application`` so the backend can re-dispatch the
    same persisted values without translation.
    """
    if teams is None:
        teams = {}
    with _task_context(self, deployment_id, "destroy", _PHASES_DESTROY) as ctx:
        try:
            ctx.mark(PHASE_STARTING, "Starting destroy")
            ctx.task_logger.resource_info(
                "deployment",
                deployment_id,
                app_id=app_id,
                git_url=app_git_link,
                release=release,
                user_vars_keys=list(user_vars.keys()),
                teams_keys=list(teams.keys()),
                action="destroy",
            )

            _task_preamble(
                ctx,
                app_git_link=app_git_link,
                release=release,
                openstack_envelope=openstack_envelope,
                envelope_error="OpenStack credential envelope missing — cannot destroy without credentials",
                clone_message="Cloning repository at original release tag",
                clone_detail=(
                    f"Cloning {app_git_link} at {release} (same ref as the original deploy "
                    "so terraform code matches the pg-backend state)"
                ),
            )

            plans, image_tag = _discover_image_plans(ctx, app_id, release, legacy_fallback=True)
            ctx.require_terraform_dir()
            terraform_vars = _terraform_var_set(
                {**user_vars["terraform"]} if "terraform" in user_vars else {},
                plans=plans,
                app_id=app_id,
                image_tag=image_tag,
                teams=teams,
                strip_files=True,
            )

            terraform = ctx.terraform()

            ctx.mark(PHASE_TERRAFORM_INIT, "Initializing Terraform")
            _require_ok(
                terraform.init(),
                op="terraform_init",
                task_logger=ctx.task_logger,
                error_message="Terraform init failed",
            )
            ctx.task_logger.success("Terraform initialization completed", category=LogCategory.STATUS)

            ctx.mark(PHASE_TERRAFORM_DESTROY, "Destroying resources")
            success, stdout, stderr = terraform.destroy(variables=terraform_vars)
            # A data source (e.g. the Glance image lookup) is re-read on every
            # destroy refresh. If that image/network was deleted out-of-band,
            # the refresh fails with "Your query returned no results" before any
            # managed resource is touched. Retry once with -refresh=false so the
            # teardown proceeds purely from state. Scoped to this exact error so
            # genuine destroy failures still surface.
            if not success and "Your query returned no results" in f"{stdout or ''}{stderr or ''}":
                ctx.task_logger.warning(
                    "Destroy blocked by a stale data source (image/network deleted "
                    "out-of-band). Retrying with -refresh=false — resources are torn "
                    "down from state.",
                    category=LogCategory.WARNING,
                )
                success, stdout, stderr = terraform.destroy(variables=terraform_vars, refresh=False)
            _require_ok(
                (success, stdout, stderr),
                op="terraform_destroy",
                task_logger=ctx.task_logger,
                error_message="Terraform destroy failed",
            )
            ctx.task_logger.success("Terraform resources destroyed", category=LogCategory.STATUS)

            ctx.mark(PHASE_CLEANUP, "Pulling final state")
            tf_state = ctx.collect_state()

            ctx.task_logger.success(f"Deployment {deployment_id} destroyed successfully", category=LogCategory.STATUS)
            ctx.task_logger.info("Destroy summary", category=LogCategory.SYSTEM, **ctx.task_logger.get_summary())

            # No outputs — destroy doesn't produce any. The field stays for
            # event-listener parity with deploy_application's payload.
            return ctx.result(tf_state=tf_state, commit_info=ctx.commit_info, outputs={})

        except Exception as e:
            _raise_failure(ctx, e, verb="Destroy", outputs={}, commit_info=ctx.commit_info)


# ----------------------------------------------------------------
# PAUSE / RESUME — compute-instance-only lifecycle
# ----------------------------------------------------------------
#
# Both tasks share the destroy preamble (git clone at the same release
# tag → per-task clouds.yaml → terraform init pointed at the pg backend)
# so we can pull the canonical terraform state and read back which
# compute instances belong to this deployment. The hot phase is a
# CLI-driven stop/start loop; terraform state is left untouched. Server
# discovery goes through the state (not tags) so no app template needs
# to opt in, and CLI idempotency lets the loop re-run safely on retry.


def _extract_compute_instance_ids(state_json: str | None) -> list[str]:
    """Return server IDs from a terraform pg-backend state dump.

    Terraform's serialised state shape is
    ``{"resources": [{"type": "...", "instances": [{"attributes": {"id": "..."}}]}]}``.
    Filtered to ``openstack_compute_instance_v2`` so we only stop/start
    Nova servers, not volumes / networks / security groups.

    Returns an empty list on any parsing trouble — the caller can then
    decide whether "no servers found" is a hard error (deploy never
    actually ran) or a no-op success (everything already torn down).
    """
    if not state_json:
        return []
    try:
        state = json.loads(state_json) if isinstance(state_json, str) else state_json
    except (TypeError, json.JSONDecodeError):
        return []

    ids: list[str] = []
    for resource in state.get("resources", []):
        if resource.get("type") != "openstack_compute_instance_v2":
            continue
        for instance in resource.get("instances", []):
            attrs = instance.get("attributes") or {}
            sid = attrs.get("id")
            if sid:
                ids.append(sid)
    return ids


def _run_compute_lifecycle(
    bound_task,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] | None,
    openstack_envelope: dict[str, Any] | None,
    *,
    action: str,  # "pause" | "resume" — only for log/error labels
    phases: tuple[str, ...],
    server_phase: str,
    server_op: str,  # "stop" | "start"
):
    """Shared body for ``pause_deployment`` / ``resume_deployment``.

    Shares :func:`destroy_deployment`'s preamble, then diverges at the hot
    phase: instead of ``terraform destroy`` it pulls the state, extracts
    every compute instance's ID and shells out to
    ``openstack server stop|start`` for each.

    Per-server failures are accumulated and re-raised once naming which
    servers failed, so the user sees "stopped 4/5; failed: web-1: locked
    task" rather than an unqualified "pause failed".
    """
    if teams is None:
        teams = {}
    with _task_context(bound_task, deployment_id, action, phases) as ctx:
        try:
            ctx.mark(PHASE_STARTING, f"Starting {action}")
            ctx.task_logger.resource_info(
                "deployment",
                deployment_id,
                app_id=app_id,
                git_url=app_git_link,
                release=release,
                action=action,
            )

            # Pause/resume never touch image variables, so the commit
            # metadata this would only be used for is not read.
            _task_preamble(
                ctx,
                app_git_link=app_git_link,
                release=release,
                openstack_envelope=openstack_envelope,
                envelope_error=f"OpenStack credential envelope missing — cannot {action} without credentials",
                clone_message="Cloning repository at original release tag",
                commit_info_mode="none",
            )

            ctx.require_terraform_dir()
            terraform = ctx.terraform()

            ctx.mark(PHASE_TERRAFORM_INIT, "Initializing Terraform")
            _require_ok(
                terraform.init(),
                op="terraform_init",
                task_logger=ctx.task_logger,
                error_message="Terraform init failed",
            )
            ctx.task_logger.success("Terraform initialization completed", category=LogCategory.STATUS)

            # Pull the canonical state from the pg backend, then walk it for
            # every compute instance in this deployment. An empty list
            # usually means the deployment never reached a successful apply —
            # a hard error, so the user isn't told pause "worked" on nothing.
            state_dump = terraform.state_pull()
            server_ids = _extract_compute_instance_ids(state_dump)
            if not server_ids:
                raise Exception(
                    "No compute instances found in terraform state — "
                    f"nothing to {action}. The deployment may have been "
                    "torn down already or never reached a successful apply."
                )
            ctx.task_logger.info(
                f"{len(server_ids)} compute instance(s) found",
                category=LogCategory.OPERATION,
                server_ids=server_ids,
            )

            ctx.mark(
                server_phase,
                f"{'Stopping' if server_op == 'stop' else 'Starting'} {len(server_ids)} server(s)",
            )
            failures = _apply_server_op(
                OpenStackService(env_vars=ctx.openstack_env),
                server_ids,
                server_op=server_op,
                task_logger=ctx.task_logger,
            )
            if failures:
                joined = "; ".join(f"{sid}: {err}" for sid, err in failures)
                raise Exception(f"{action} failed for {len(failures)}/{len(server_ids)} server(s): {joined}")

            ctx.mark(PHASE_CLEANUP, "Pulling final state snapshot")
            # State doesn't change for pause/resume (the resources still
            # exist, just in a different power state), but we pull it again
            # so the task row gets a fresh snapshot for debugging.
            try:
                tf_state_post = terraform.state_pull()
            except Exception as e:
                ctx.task_logger.warning(
                    f"Could not pull terraform state post-{action}: {e}",
                    category=LogCategory.WARNING,
                )
                tf_state_post = state_dump

            ctx.task_logger.success(
                f"Deployment {deployment_id} {action}d successfully",
                category=LogCategory.STATUS,
            )
            # Pause/resume neither generate nor change terraform outputs;
            # the field stays for event-listener parity.
            return ctx.result(tf_state=tf_state_post, commit_info=None, outputs={})

        except Exception as e:
            # Unlike the other three tasks, no state snapshot is pulled on
            # failure here: pause/resume never modify state, so a snapshot
            # would only cost another subprocess on the error path.
            _raise_failure(ctx, e, verb=action, collect_state=False, outputs={}, commit_info=None)


def _apply_server_op(
    openstack_service: OpenStackService,
    server_ids: list[str],
    *,
    server_op: str,
    task_logger: Any,
) -> list[tuple[str, str]]:
    """Run stop/start against every server, returning the ones that failed.

    The CLI is idempotent for both operations, so a retry of the whole
    task is safe. ``server_show`` is a cosmetic pre-flight — it makes the
    log read "web-1 (ACTIVE)" instead of a bare UUID — and never fails
    the operation.
    """
    op_method = openstack_service.server_stop if server_op == "stop" else openstack_service.server_start
    failures: list[tuple[str, str]] = []
    for sid in server_ids:
        info = openstack_service.server_show(sid)
        label = info.get("name", sid) if info else sid
        task_logger.info(
            f"{server_op} {label} (status: {(info.get('status') if info else None) or 'unknown'})",
            category=LogCategory.OPERATION,
            server_id=sid,
        )
        ok, err = op_method(sid)
        if ok:
            task_logger.success(f"{label}: {server_op} OK", category=LogCategory.STATUS)
        else:
            task_logger.error(
                f"{label}: {server_op} failed: {err}",
                category=LogCategory.ERROR,
                server_id=sid,
            )
            failures.append((sid, err or "unknown error"))
    return failures


@celery_app.task(bind=True, name="tasks.pause_deployment")
def pause_deployment(
    self,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] = None,
    openstack_envelope: dict[str, Any] | None = None,
):
    """Halt a deployment by stopping all of its compute instances.

    Volumes and networks are untouched, so resume restores the same
    instances byte-for-byte. The terraform state is also untouched,
    so a subsequent destroy proceeds normally (terraform destroy is
    happy to tear down SHUTOFF instances).
    """
    return _run_compute_lifecycle(
        self,
        deployment_id,
        app_id,
        app_git_link,
        release,
        user_vars,
        teams,
        openstack_envelope,
        action="pause",
        phases=_PHASES_PAUSE,
        server_phase=PHASE_SERVER_STOP,
        server_op="stop",
    )


@celery_app.task(bind=True, name="tasks.resume_deployment")
def resume_deployment(
    self,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] = None,
    openstack_envelope: dict[str, Any] | None = None,
):
    """Resume a paused deployment by starting all of its compute instances.

    Mirrors :func:`pause_deployment`'s preamble exactly so the two
    code paths stay symmetric and easy to compare side-by-side.
    """
    return _run_compute_lifecycle(
        self,
        deployment_id,
        app_id,
        app_git_link,
        release,
        user_vars,
        teams,
        openstack_envelope,
        action="resume",
        phases=_PHASES_RESUME,
        server_phase=PHASE_SERVER_START,
        server_op="start",
    )


# ----------------------------------------------------------------
# REDEPLOY ONE RESOURCE
# ----------------------------------------------------------------
#
# Replace exactly one compute instance via
# ``terraform apply -replace=<addr> -target=<addr>``. Everything else in
# the deployment stays untouched. The backend whitelists the address
# against the cached TF state before dispatch; we re-check the address
# shape here as defense in depth. Same per-task clouds.yaml + pg backend
# schema as deploy/destroy, so the apply sees the same state file.

_REDEPLOY_ADDRESS_RE = re.compile(
    r"""^
    [A-Za-z_][A-Za-z0-9_]*
    \.[A-Za-z_][A-Za-z0-9_-]*
    (?:\[(?:\d+|"[^"\\]+")\])?
    $""",
    re.VERBOSE,
)


def _build_current_roster(teams: dict[str, list]) -> tuple[set[str], set[str]]:
    """Compute the legal slot-key sets for the current roster.

    Returns a ``(team_keys, user_keys)`` tuple:

    * ``team_keys`` — every team name currently present. These are the
      valid slot keys for ``var_scope=team``.
    * ``user_keys`` — composite ``"<team>-<email>"`` keys for every
      user currently rostered to a team. These are the valid slot keys
      for ``var_scope=user``.

    Roster entries can either be plain strings (email addresses) or
    dicts with an ``email`` key — matches the shape the backend ships
    in ``teams`` (see ``_attach_files_to_user_input``). Anything else is
    skipped defensively.
    """
    team_keys: set[str] = set()
    user_keys: set[str] = set()
    for team_name, members in (teams or {}).items():
        if not team_name:
            continue
        team_keys.add(team_name)
        if not isinstance(members, list):
            continue
        for member in members:
            email = member if isinstance(member, str) else (member.get("email") if isinstance(member, dict) else None)
            if not email:
                continue
            user_keys.add(f"{team_name}-{email}")
    return team_keys, user_keys


def _reconcile_scoped_vars_to_roster(
    terraform_vars: dict[str, Any],
    teams: dict[str, list],
    task_logger: Any,
) -> dict[str, Any]:
    """Drop scoped-map entries whose slot keys no longer match the roster.

    Redeploy replays the originally-persisted ``user_vars["terraform"]``
    blob, but the team/user roster may have shifted since the initial
    deploy (members added or removed, teams renamed). A scoped variable
    keyed on the old roster would then ship Terraform a map containing
    orphan keys — at best a noisy diff, at worst a type/required
    failure that blocks the replace.

    Heuristic: a value is considered scoped when it's a non-empty
    ``dict`` whose keys form a subset of either the team-name roster
    (``var_scope=team``) or the ``<team>-<user>`` composite roster
    (``var_scope=user``). On match we intersect the value's keys with
    the current roster and drop the orphans. Maps that don't match the
    heuristic — e.g. file-shape vars or the ``users`` injection — are
    left untouched. Every drop is announced in the task log so the
    operator sees which slots were retired.

    A value that becomes empty after intersection is dropped from the
    var-set entirely; Terraform validation handles the
    missing-required case from there (it can pick up a declared
    default or surface the required-but-missing error properly).
    """
    if not terraform_vars:
        return terraform_vars

    team_keys, user_keys = _build_current_roster(teams)
    if not team_keys and not user_keys:
        # Nothing rostered — can't reconcile, leave the var-set alone.
        return terraform_vars

    reconciled: dict[str, Any] = {}
    for name, value in terraform_vars.items():
        # Only dict-shaped, non-empty values can be scoped maps. Skip
        # the ``users`` injection — we set that ourselves from ``teams``
        # right after this and it's not an app-defined scoped var.
        if name == "users" or not isinstance(value, dict) or not value:
            reconciled[name] = value
            continue
        # File-shape values (see ``_looks_like_file_var_value``) are
        # already keyed by slot but use a different content contract;
        # let the regular file-strip handle them.
        if _looks_like_file_var_value(value):
            reconciled[name] = value
            continue

        slot_keys = set(value.keys())
        # Pick the roster axis whose universe best matches the slot
        # keys. Subset wins outright; otherwise pick the axis with the
        # larger overlap so a partially-stale map still gets cleaned.
        team_overlap = slot_keys & team_keys
        user_overlap = slot_keys & user_keys
        if slot_keys <= team_keys and team_keys:
            allowed = team_keys
        elif slot_keys <= user_keys and user_keys or len(user_overlap) >= len(team_overlap) and user_overlap:
            allowed = user_keys
        elif team_overlap:
            allowed = team_keys
        else:
            # No overlap with either roster axis — leave the value
            # alone. Probably a non-scoped map(string,...) variable
            # the user explicitly populated.
            reconciled[name] = value
            continue

        kept = {k: v for k, v in value.items() if k in allowed}
        dropped = sorted(slot_keys - allowed)
        if dropped:
            task_logger.warning(
                f"Redeploy roster reconciliation: dropped {len(dropped)} "
                f"orphan slot(s) from variable '{name}': {dropped}",
                category=LogCategory.WARNING,
                variable=name,
                dropped_slots=dropped,
            )
        if kept:
            reconciled[name] = kept
        else:
            # All slots orphaned — drop the var entirely so terraform
            # validation can fall back to the declared default (if any)
            # or surface a proper required-but-missing error.
            task_logger.warning(
                f"Redeploy roster reconciliation: variable '{name}' has "
                "no surviving slots after roster intersection — falling "
                "back to its declared default (or required-but-missing).",
                category=LogCategory.WARNING,
                variable=name,
            )
    return reconciled


@celery_app.task(bind=True, name="tasks.redeploy_resource")
def redeploy_resource(
    self,
    deployment_id: str,
    app_id: str,
    app_git_link: str,
    release: str,
    user_vars: dict[str, Any],
    teams: dict[str, list] = None,
    openstack_envelope: dict[str, Any] | None = None,
    resource_address: str | None = None,
):
    """Replace ONE compute instance via ``-target`` + ``-replace``.

    Args mirror ``deploy_application`` so the backend's
    ``_dispatch_lifecycle_task`` can ship the same persisted state. The
    extra ``resource_address`` carries the Terraform state address (e.g.
    ``openstack_compute_instance_v2.team_ide["Team-A"]``) the user clicked.

    Returns the same payload shape as deploy/destroy so the celery event
    listener stays generic.
    """
    if teams is None:
        teams = {}
    with _task_context(self, deployment_id, "redeploy", _PHASES_REDEPLOY) as ctx:
        tf_state: str | None = None
        outputs: dict[str, Any] | None = None
        try:
            # Validate the address shape before doing any work. The backend
            # already whitelisted it against the cached state; this is
            # defense in depth against a malformed value reaching the CLI.
            if not resource_address or not _REDEPLOY_ADDRESS_RE.match(resource_address):
                raise Exception(f"redeploy_resource called with invalid resource_address: {resource_address!r}")

            ctx.mark(PHASE_STARTING, f"Starting redeploy of {resource_address}")
            ctx.task_logger.resource_info(
                "deployment",
                deployment_id,
                app_id=app_id,
                git_url=app_git_link,
                release=release,
                user_vars_keys=list(user_vars.keys()),
                teams_keys=list(teams.keys()),
                action="redeploy",
                resource_address=resource_address,
            )

            _task_preamble(
                ctx,
                app_git_link=app_git_link,
                release=release,
                openstack_envelope=openstack_envelope,
                envelope_error="OpenStack credential envelope missing — cannot redeploy without credentials",
                clone_message="Cloning repository at original release tag",
                clone_detail=(
                    f"Cloning {app_git_link} at {release} (same ref as the original deploy "
                    "so terraform code matches the pg-backend state)"
                ),
                commit_info_mode="success_only",
            )

            plans, image_tag = _discover_image_plans(ctx, app_id, release, legacy_fallback=True)
            ctx.require_terraform_dir()

            # File variables are KEPT here: ``apply -replace`` recreates the
            # targeted VM, so cloud-init runs fresh and needs the original
            # write_files payload. The persisted user_vars were keyed on the
            # roster at deploy time and membership may have shifted since, so
            # scoped variables are reconciled against the current roster
            # first — otherwise terraform chokes on orphan slot keys.
            terraform_vars = _terraform_var_set(
                {**user_vars["terraform"]} if "terraform" in user_vars else {},
                plans=plans,
                app_id=app_id,
                image_tag=image_tag,
                teams=teams,
                transform=lambda raw: _reconcile_scoped_vars_to_roster(raw, teams, ctx.task_logger),
            )

            terraform = ctx.terraform()

            ctx.mark(PHASE_TERRAFORM_INIT, "Initializing Terraform")
            _require_ok(
                terraform.init(),
                op="terraform_init",
                task_logger=ctx.task_logger,
                error_message="Terraform init failed",
            )
            ctx.task_logger.success("Terraform initialization completed", category=LogCategory.STATUS)

            ctx.mark(PHASE_TERRAFORM_APPLY, f"Applying replace for {resource_address}")
            # ``-replace`` taints the single resource so terraform plans a
            # destroy+create on it; ``-target`` scopes the apply to that
            # resource and its dependencies, leaving the rest untouched.
            _require_ok(
                terraform.apply(
                    variables=terraform_vars,
                    targets=[resource_address],
                    replace=[resource_address],
                ),
                op="terraform_apply",
                task_logger=ctx.task_logger,
                error_message="Terraform apply (replace) failed",
            )
            ctx.task_logger.success(f"Resource {resource_address} replaced", category=LogCategory.STATUS)

            ctx.mark(PHASE_CLEANUP, "Pulling final state")
            tf_state = ctx.collect_state()
            outputs = ctx.collect_outputs()

            ctx.task_logger.success(
                f"Deployment {deployment_id} redeploy of {resource_address} completed",
                category=LogCategory.STATUS,
            )
            return ctx.result(tf_state=tf_state, outputs=outputs or {}, commit_info=ctx.commit_info)

        except Exception as e:
            _raise_failure(
                ctx, e, verb="Redeploy", tf_state=tf_state, outputs=outputs or {}, commit_info=ctx.commit_info
            )
