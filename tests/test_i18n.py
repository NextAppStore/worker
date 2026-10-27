"""Sprache der Worker-Texte: Katalog, Übersetzung, Logger und Task-Anbindung."""

import ast
import itertools
import json
import os
import pathlib

import pytest

from app.i18n import localize_text, normalize_locale, t
from app.i18n.catalog import MESSAGES
from app.i18n.translate import find_key, placeholders
from app.tasks import Failure, deploy_application, pause_deployment
from app.utils.logger import LogCategory, StructuredLogger
from tests.test_tasks_branches import (
    _Events,
    _FakeTemplate,
    _make_build_lock_mock,
    _make_clouds_config_mock,
    _make_git_mock,
    _make_openstack_service_mock,
    _make_packer_mock,
    _make_tofu_executor_mock,
    _patch_git_repo,
    _seed_terraform_dir,
)

APP_DIR = pathlib.Path(__file__).resolve().parents[1] / "app"


# ---------------------------------------------------------------------------
# Sprache aus dem Task-Payload
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value, expected",
    [
        ("de", "de"),
        ("en", "en"),
        ("EN", "en"),
        ("de-DE", "de"),
        ("en_US", "en"),
        ("fr", "de"),
        ("", "de"),
        (None, "de"),
        (3, "de"),
    ],
)
def test_normalize_locale(value, expected):
    assert normalize_locale(value) == expected


# ---------------------------------------------------------------------------
# Katalog
# ---------------------------------------------------------------------------
def test_every_entry_has_both_languages_with_the_same_placeholders():
    for key, entry in MESSAGES.items():
        assert entry["en"].strip(), key
        assert entry["de"].strip(), key
        assert sorted(placeholders(entry["en"])) == sorted(placeholders(entry["de"])), key


def test_en_templates_are_unambiguous():
    seen: dict[str, str] = {}
    for key, entry in MESSAGES.items():
        assert entry["en"] not in seen, f"{key} dupliziert {seen[entry['en']]}"
        seen[entry["en"]] = key


def test_t_formats_parameters_and_never_raises():
    assert t("log.repo_cleanup_failed", "de", error="boom") == "Aufräumen des Repositorys fehlgeschlagen: boom"
    assert t("log.repo_cleanup_failed", "en", error="boom") == "Repository cleanup failed: boom"
    assert t("does.not.exist", "de") == "does.not.exist"
    assert "{error}" in t("log.repo_cleanup_failed", "en")  # fehlender Parameter: unformatiert


# ---------------------------------------------------------------------------
# localize_text
# ---------------------------------------------------------------------------
def test_known_text_is_translated_and_english_is_untouched():
    assert localize_text("Repository cleanup completed", "de") == "Repository aufgeräumt"
    assert localize_text("Repository cleanup completed", "en") == "Repository cleanup completed"


def test_parameters_are_extracted():
    assert localize_text("Could not read OpenTofu outputs: connection refused", "de") == (
        "OpenTofu-Ausgaben konnten nicht gelesen werden: connection refused"
    )


def test_log_prefix_may_be_empty():
    assert localize_text("Packer init failed", "de") == "Packer-Initialisierung fehlgeschlagen"
    assert localize_text("[web] Packer init failed", "de") == "[web] Packer-Initialisierung fehlgeschlagen"


def test_action_values_are_restricted_and_translated():
    assert localize_text("Starting pause", "de") == "Pausieren wird gestartet"
    assert localize_text("Deployment d1 resumed successfully", "de") == (
        "Deployment d1: Fortsetzen erfolgreich abgeschlossen"
    )
    # ``{action}`` darf nicht beliebige Texte an sich reißen.
    assert localize_text("Starting 3 server(s)", "de") == "3 Server werden gestartet"
    assert localize_text("Frobnicate failed: boom", "de") == "Frobnicate failed: boom"


def test_nested_messages_are_translated_recursively():
    nested = "Packer error: Packer build failed: exit 1"
    assert localize_text(nested, "de") == "Packer-Fehler: Packer-Build fehlgeschlagen: exit 1"


def test_foreign_text_is_left_alone():
    raw = "Error: Invalid provider configuration"
    assert localize_text(raw, "de") == raw


def test_specific_template_wins_over_a_generic_one():
    assert localize_text("Git clone failed: Git command failed: fatal", "de") == (
        "Git-Clone fehlgeschlagen: Git-Befehl fehlgeschlagen: fatal"
    )


# ---------------------------------------------------------------------------
# Vollständigkeit: jeder selbst formulierte Text im Code hat einen Eintrag
# ---------------------------------------------------------------------------
_LEVELS = {"debug", "info", "success", "warning", "error", "exception"}
_SERVICE_RETURNS = {"opentofu_executor.py", "packer_executor.py", "openstack_service.py"}
_RAISED = {"Exception", "CredentialEnvelopeError", "TimeoutError", "PackerTemplateDiscoveryError"}
# Beispielwerte für Ausdrücke, deren Platzhalter nur bestimmte Texte annehmen.
_SAMPLES = {
    "action": ["pause", "resume"],
    "server_op": ["stop", "start"],
    "log_prefix": ["", "[web] "],
    "'Stopping' if server_op == 'stop' else 'Starting'": ["Stopping", "Starting"],
}


def _renderings(node: ast.AST) -> list[str] | None:
    """Mögliche Texte eines Literals; Ausdrücke werden durch Beispielwerte ersetzt."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _renderings(node.left), _renderings(node.right)
        if left is None or right is None:
            return None
        return [a + b for a, b in itertools.product(left, right)]
    if isinstance(node, ast.JoinedStr):
        parts: list[list[str]] = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append([value.value])
            else:
                parts.append(_SAMPLES.get(ast.unparse(value.value), ["x"]))
        return ["".join(combo) for combo in itertools.product(*parts)]
    return None


def _returned_texts(node: ast.Return, filename: str) -> list[ast.AST]:
    """Text-Literale, die eine Service-Funktion zurückgibt (auch als Tupel-Element)."""
    value = node.value
    if value is None:
        return []
    is_tuple = isinstance(value, ast.Tuple)
    texts: list[ast.AST] = []
    for part in value.elts if is_tuple else [value]:
        if isinstance(part, ast.BoolOp) and isinstance(part.op, ast.Or):
            part = part.values[-1]  # ``stderr.strip() or "openstack server stop failed"``
        if isinstance(part, ast.JoinedStr) and filename == "openstack_service.py":
            texts.append(part)
        elif isinstance(part, ast.Constant) and isinstance(part.value, str) and part.value.strip():
            # Ein einzelner zurückgegebener String in opentofu_executor ist die .tf-Vorlage, kein Text.
            is_template = filename == "opentofu_executor.py" and not is_tuple
            if not is_template:
                texts.append(part)
    return texts


def _self_written_texts() -> list[tuple[str, int, str]]:
    found: list[tuple[str, int, str]] = []
    for path in sorted(APP_DIR.rglob("*.py")):
        if "i18n" in path.parts:
            continue
        rel = str(path.relative_to(APP_DIR.parent))
        for node in ast.walk(ast.parse(path.read_text())):
            candidates: list[ast.AST] = []
            if isinstance(node, ast.Call):
                name = ast.unparse(node.func)
                if name.startswith("task_logger.") and name.split(".")[-1] in _LEVELS and node.args:
                    candidates.append(node.args[0])
                elif name == "phase_tracker.mark" and len(node.args) > 1:
                    candidates.append(node.args[1])
            elif (
                isinstance(node, ast.BoolOp)
                and isinstance(node.op, ast.Or)
                and path.name == "tasks.py"
                and isinstance(node.values[-1], ast.Constant)
                and node.values[-1].value == "unknown error"
            ):
                candidates.append(node.values[-1])
            elif (
                isinstance(node, ast.Assign)
                and path.name == "git_service.py"
                and any(isinstance(t, ast.Name) and t.id == "error_msg" for t in node.targets)
            ):
                # ``error_msg = f"..."`` wird anschließend als ``Exception(error_msg)`` geworfen.
                candidates.append(node.value)
            elif isinstance(node, ast.Return) and path.name in _SERVICE_RETURNS:
                # Services melden Fehler als Rückgabewert (``(False, "...")``); die Aufrufer
                # setzen den Text in Log-Zeilen und ``Failure``-Meldungen ein.
                candidates.extend(_returned_texts(node, path.name))
            elif isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call) and node.exc.args:
                if ast.unparse(node.exc.func) in _RAISED:
                    candidates.append(node.exc.args[0])
            for cand in candidates:
                for text in _renderings(cand) or []:
                    found.append((rel, cand.lineno, text))
    return found


def test_every_self_written_text_has_a_catalog_entry():
    texts = _self_written_texts()
    assert len(texts) > 100, "AST-Scan findet auffällig wenige Texte"
    missing = [f"{f}:{line}  {text!r}" for f, line, text in texts if find_key(text) is None]
    assert not missing, "Katalogeintrag fehlt für:\n  " + "\n  ".join(missing)


def test_every_catalog_entry_is_used_by_the_code():
    """Verwaiste Einträge sind meist ein Tippfehler im EN-Text."""
    used = {find_key(text) for _, _, text in _self_written_texts()}
    # Aus dem Logger selbst (``t(...)``), nicht aus einem Aufruf mit Literal.
    used |= {"log.operation_start", "log.operation_completed", "log.operation_failed"}
    unused = sorted(set(MESSAGES) - used)
    assert not unused, f"Nicht verwendet: {unused}"


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
def _messages(logger):
    return [e["message"] for e in logger.get_logs_dict()]


def test_logger_translates_its_own_texts_in_german():
    logger = StructuredLogger("t", console=False, locale="de")
    logger.success("Repository cleanup completed", category=LogCategory.SYSTEM)
    logger.warning("Could not read OpenTofu outputs: boom")
    logger.error("OpenTofu plan failed")
    assert _messages(logger) == [
        "Repository aufgeräumt",
        "OpenTofu-Ausgaben konnten nicht gelesen werden: boom",
        "OpenTofu-Plan fehlgeschlagen",
    ]


def test_logger_default_is_untranslated():
    """Logger ohne Task (Module, Tests) bleiben Englisch."""
    logger = StructuredLogger("t", console=False)
    logger.success("Repository cleanup completed")
    assert _messages(logger) == ["Repository cleanup completed"]


def test_logger_translates_exception_text_but_keeps_exception_details():
    logger = StructuredLogger("t", console=False, locale="de")
    logger.exception("Deployment failed: boom", exception=ValueError("boom"))
    entry = logger.get_logs_dict()[0]
    assert entry["message"] == "Deployment fehlgeschlagen: boom"
    # Technische Felder bleiben, wie sie sind.
    assert entry["exception_message"] == "boom"
    assert entry["exception_type"] == "ValueError"


def test_logger_does_not_touch_tool_output():
    logger = StructuredLogger("t", console=False, locale="de")
    logger.tool_output_line("tofu", "Repository cleanup completed")
    logger.command_output("tofu", "Repository cleanup completed")
    assert _messages(logger) == ["Repository cleanup completed", "Repository cleanup completed"]


def test_logger_keeps_structure_when_translating():
    logger = StructuredLogger("t", console=False, locale="de")
    logger.operation_start("git_clone")
    logger.operation_end("git_clone")
    logger.phase("GIT_CLONE")
    start, end, phase = logger.get_logs_dict()
    assert start["message"] == "Start: git_clone" and start["operation"] == "git_clone"
    assert end["message"] == "Abgeschlossen: git_clone" and end["operation"] == "git_clone"
    # Phasen-Bezeichner sind Schlüssel für das Frontend und bleiben unverändert.
    assert phase["message"] == "=== GIT_CLONE ===" and phase["phase"] == "GIT_CLONE"
    assert {start["category"], end["category"], phase["category"]} == {"operation", "phase"}


def test_progress_message_is_translated_but_phase_name_is_not():
    events = []
    logger = StructuredLogger(
        "t", console=False, locale="de", event_emitter=lambda name, payload: events.append(payload)
    )
    logger.progress("GIT_CLONE", 3, 10, "Cloning repository")
    assert events[0]["message"] == "Repository wird geklont"
    assert events[0]["phase"] == "GIT_CLONE"
    assert events[0]["progress_pct"] == 30


# ---------------------------------------------------------------------------
# Task-Anbindung (Header -> Logger und Failure)
# ---------------------------------------------------------------------------
def _failing_deploy(mocker, tmp_path, locale=None):
    repo_path = str(tmp_path / "repo")
    os.makedirs(repo_path)
    _seed_terraform_dir(repo_path)
    _make_git_mock(mocker, repo_path)
    _patch_git_repo(mocker)
    _make_clouds_config_mock(mocker)
    mocker.patch("app.tasks._discover_packer_templates", return_value=[_FakeTemplate("default")])
    _make_openstack_service_mock(mocker, image_exists=False)
    _make_build_lock_mock(mocker, held=True)
    _make_packer_mock(mocker, build=False)
    _make_tofu_executor_mock(mocker)
    with pytest.raises(Failure) as excinfo:
        deploy_application(
            _Events(),
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={"terraform": {}},
            teams=None,
            openstack_envelope={"project_id": "p1"},
            locale=locale,
        )
    return excinfo.value.to_dict()


@pytest.mark.parametrize(
    "locale, error_prefix, log_text",
    [
        ("en", "Packer error: Packer build failed", "Cloning repository: https://git/repo.git"),
        (
            "de",
            "Packer-Fehler: Packer-Build fehlgeschlagen",
            "Repository wird geklont: https://git/repo.git",
        ),
        (None, "Packer-Fehler: Packer-Build fehlgeschlagen", "Repository wird geklont: https://git/repo.git"),
    ],
)
def test_failure_message_and_logs_follow_the_locale(mocker, tmp_path, locale, error_prefix, log_text):
    payload = _failing_deploy(mocker, tmp_path, locale=locale)
    assert payload["error"].startswith(error_prefix)
    assert log_text in [entry["message"] for entry in payload["logs"]]


def test_failure_payload_structure_is_independent_of_the_language(mocker, tmp_path):
    """Das Backend parst die Struktur; nur Textwerte dürfen sich unterscheiden."""
    de = _failing_deploy(mocker, tmp_path / "de", locale="de")
    en = _failing_deploy(mocker, tmp_path / "en", locale="en")
    assert de.keys() == en.keys()
    assert len(de["logs"]) == len(en["logs"])
    for d, e in zip(de["logs"], en["logs"], strict=True):
        assert d.keys() == e.keys()
        assert (d["level"], d["category"], d.get("phase"), d.get("tool")) == (
            e["level"],
            e["category"],
            e.get("phase"),
            e.get("tool"),
        )


def _failing_pause(mocker, tmp_path, locale=None):
    """Pause, bei der ein Server mit einem selbst formulierten Service-Fehler ausfällt."""
    repo_path = str(tmp_path / "repo")
    os.makedirs(repo_path)
    _seed_terraform_dir(repo_path)
    _make_git_mock(mocker, repo_path)
    _make_clouds_config_mock(mocker)
    state = json.dumps(
        {
            "resources": [
                {
                    "type": "openstack_compute_instance_v2",
                    "instances": [{"attributes": {"id": "ok-1"}}, {"attributes": {"id": "bad-1"}}],
                }
            ]
        }
    )
    _make_tofu_executor_mock(mocker, state=state)
    os_inst = mocker.MagicMock()
    os_inst.server_show.return_value = {"name": "n", "status": "ACTIVE"}
    # Derselbe Text, den ``OpenStackService.server_stop`` bei leerem stderr liefert.
    os_inst.server_stop.side_effect = [(True, None), (False, "openstack server stop failed")]
    mocker.patch("app.tasks.OpenStackService", return_value=os_inst)
    with pytest.raises(Failure) as excinfo:
        pause_deployment(
            _Events(),
            deployment_id="dep-1",
            app_id="myapp",
            app_git_link="https://git/repo.git",
            release="v1",
            user_vars={},
            teams=None,
            openstack_envelope={"project_id": "p1"},
            locale=locale,
        )
    return excinfo.value.to_dict()


def test_pause_failure_is_german_including_the_service_error(mocker, tmp_path):
    payload = _failing_pause(mocker, tmp_path, locale="de")
    assert payload["error"] == "Pausieren fehlgeschlagen für 1/2 Server: bad-1: openstack server stop fehlgeschlagen"
    messages = [entry["message"] for entry in payload["logs"]]
    assert any(m.endswith("Stoppen fehlgeschlagen: openstack server stop fehlgeschlagen") for m in messages)


def test_pause_failure_stays_english_for_en(mocker, tmp_path):
    payload = _failing_pause(mocker, tmp_path, locale="en")
    assert payload["error"] == "pause failed for 1/2 server(s): bad-1: openstack server stop failed"
