"""Nachrichtenkatalog (DE/EN) für die Texte, die der Worker selbst formuliert.

Jeder Eintrag hat einen stabilen Key und beide Sprachen. Platzhalter folgen
``str.format`` (``{name}``; wörtliche Klammern als ``{{`` / ``}}``).

**EN ist der Quelltext.** Die Texte in ``tasks.py`` und den Services bleiben
Englisch und werden beim Ausgeben anhand ihres EN-Musters nachgeschlagen
(``app/i18n/translate.py``). Das betrifft:

* ``task_logger.info/success/warning/error/exception(...)``
* den Text von ``phase_tracker.mark(...)``
* die Meldungen von ``raise Exception(...)`` & Co., die als ``Failure`` beim
  Backend ankommen

Neuer Text im Code? Mit gleichem Wortlaut hier eintragen.
``tests/test_i18n.py`` schlägt fehl, wenn ein Text im Code keinen Eintrag hat.

Rückgabewerte der Services (``(False, "openstack server stop failed")``), die der
Task in Log-Zeilen und ``Failure``-Meldungen einsetzt, stehen ebenfalls hier.

Nicht übersetzt wird bewusst: die Roh-Ausgabe von OpenTofu und Packer, die
Phasen-Bezeichner (``GIT_CLONE`` …), Feldnamen im Log-Eintrag und Texte aus
Fremdsystemen (Git, OpenStack), die in ``{error}`` & Co. landen. Ebenso bleiben
die Konsolen-Logs der Services (modulweiter ``logger`` in ``app/services/``)
Englisch: Sie gehen nur ins Container-Log und erreichen die Nutzer*in nie, weil
nur der ``task_logger`` an den Celery-Eventbus und in die ``Failure``-Payload
angebunden ist.
"""

from __future__ import annotations

from typing import TypedDict


class Message(TypedDict):
    en: str
    de: str


MESSAGES: dict[str, Message] = {
    # ------------------------------------------------------------------
    # Phasen-Texte (phase_tracker.mark)
    # ------------------------------------------------------------------
    "phase.starting_deployment": {"en": "Starting deployment", "de": "Deployment wird gestartet"},
    "phase.starting_destroy": {"en": "Starting destroy", "de": "Löschen wird gestartet"},
    "phase.starting_action": {"en": "Starting {action}", "de": "{action} wird gestartet"},
    "phase.starting_redeploy": {
        "en": "Starting redeploy of {resource_address}",
        "de": "Neu-Deployment von {resource_address} wird gestartet",
    },
    "phase.validate_credentials": {
        "en": "Validating OpenStack credentials",
        "de": "OpenStack-Zugangsdaten werden geprüft",
    },
    "phase.clone": {"en": "Cloning repository", "de": "Repository wird geklont"},
    "phase.clone_original_tag": {
        "en": "Cloning repository at original release tag",
        "de": "Repository wird beim ursprünglichen Release-Tag geklont",
    },
    "phase.write_clouds_yaml": {
        "en": "Writing per-task clouds.yaml",
        "de": "Aufgabenspezifische clouds.yaml wird geschrieben",
    },
    "phase.tofu_init": {"en": "Initializing Terraform", "de": "Terraform wird initialisiert"},
    "phase.tofu_plan": {
        "en": "Planning Terraform deployment",
        "de": "Terraform-Deployment wird geplant",
    },
    "phase.tofu_apply": {
        "en": "Applying configuration (this may take minutes)",
        "de": "Konfiguration wird angewendet (kann einige Minuten dauern)",
    },
    "phase.collect_outputs": {"en": "Collecting outputs", "de": "Ausgaben werden gesammelt"},
    "phase.destroy_resources": {"en": "Destroying resources", "de": "Ressourcen werden gelöscht"},
    "phase.cleanup": {"en": "Cleaning up", "de": "Aufräumen"},
    "phase.final_state": {"en": "Pulling final state", "de": "Endgültiger State wird gelesen"},
    "phase.server_stop": {"en": "Stopping {count} server(s)", "de": "{count} Server werden gestoppt"},
    "phase.server_start": {"en": "Starting {count} server(s)", "de": "{count} Server werden gestartet"},
    "phase.redeploy_apply": {
        "en": "Applying replace for {resource_address}",
        "de": "Austausch von {resource_address} wird angewendet",
    },
    "phase.packer_init": {
        "en": "{log_prefix}Initializing Packer plugins",
        "de": "{log_prefix}Packer-Plugins werden initialisiert",
    },
    "phase.packer_validate": {
        "en": "{log_prefix}Validating Packer template",
        "de": "{log_prefix}Packer-Template wird validiert",
    },
    "phase.packer_build": {
        "en": "{log_prefix}Building image '{image_name}' (this may take minutes)",
        "de": "{log_prefix}Image '{image_name}' wird gebaut (kann einige Minuten dauern)",
    },
    # ------------------------------------------------------------------
    # Log-Zeilen (task_logger.*)
    # ------------------------------------------------------------------
    "log.operation_start": {"en": "Starting: {operation}", "de": "Start: {operation}"},
    "log.operation_completed": {"en": "Completed: {operation}", "de": "Abgeschlossen: {operation}"},
    "log.operation_failed": {"en": "Failed: {operation}", "de": "Fehlgeschlagen: {operation}"},
    "log.envelope_received": {
        "en": "OpenStack credential envelope received",
        "de": "OpenStack-Zugangsdaten empfangen",
    },
    "log.cloning": {"en": "Cloning repository: {app_git_link}", "de": "Repository wird geklont: {app_git_link}"},
    "log.cloning_same_ref": {
        "en": "Cloning {app_git_link} at {release} (same ref as the original deploy so OpenTofu code "
        "matches the stored state)",
        "de": "{app_git_link} wird bei {release} geklont (gleiche Referenz wie beim ursprünglichen "
        "Deployment, damit der OpenTofu-Code zum gespeicherten State passt)",
    },
    "log.repo_cloned": {"en": "Repository cloned", "de": "Repository geklont"},
    "log.repo_cloned_commit": {
        "en": "Repository cloned at commit {commit}",
        "de": "Repository bei Commit {commit} geklont",
    },
    "log.clouds_yaml_written": {
        "en": "Per-task clouds.yaml written",
        "de": "Aufgabenspezifische clouds.yaml geschrieben",
    },
    "log.clouds_yaml_cleanup_failed": {
        "en": "Per-task clouds.yaml cleanup failed: {error}",
        "de": "Aufräumen der aufgabenspezifischen clouds.yaml fehlgeschlagen: {error}",
    },
    "log.repo_cleanup_done": {"en": "Repository cleanup completed", "de": "Repository aufgeräumt"},
    "log.repo_cleanup_failed": {
        "en": "Repository cleanup failed: {error}",
        "de": "Aufräumen des Repositorys fehlgeschlagen: {error}",
    },
    "log.commit_info_failed": {
        "en": "Could not extract commit info: {error}",
        "de": "Commit-Informationen konnten nicht gelesen werden: {error}",
    },
    "log.outputs_read_failed": {
        "en": "Could not read OpenTofu outputs: {error}",
        "de": "OpenTofu-Ausgaben konnten nicht gelesen werden: {error}",
    },
    "log.image_exists": {
        "en": "{log_prefix}Image '{image_name}' already exists (ID: {image_id}). Skipping Packer build.",
        "de": "{log_prefix}Image '{image_name}' existiert bereits (ID: {image_id}). Packer-Build wird übersprungen.",
    },
    "log.image_built_elsewhere": {
        "en": "{log_prefix}Image '{image_name}' built by another worker (ID: {image_id}). Skipping.",
        "de": "{log_prefix}Image '{image_name}' wurde von einem anderen Worker gebaut (ID: {image_id}). "
        "Wird übersprungen.",
    },
    "log.image_waiting": {
        "en": "{log_prefix}Another worker is currently building image '{image_name}'. Waiting…",
        "de": "{log_prefix}Ein anderer Worker baut gerade das Image '{image_name}'. Warte…",
    },
    "log.image_missing": {
        "en": "{log_prefix}Image '{image_name}' does not exist. Building...",
        "de": "{log_prefix}Image '{image_name}' existiert nicht. Wird gebaut...",
    },
    "log.image_built": {
        "en": "{log_prefix}Image '{image_name}' built successfully",
        "de": "{log_prefix}Image '{image_name}' erfolgreich gebaut",
    },
    "log.packer_var_keys": {
        "en": "{log_prefix}Packer variable keys",
        "de": "{log_prefix}Packer-Variablenschlüssel",
    },
    "log.no_packer_template": {
        "en": "No Packer template found, skipping image build",
        "de": "Kein Packer-Template gefunden, Image-Build wird übersprungen",
    },
    "log.tofu_var_keys": {"en": "OpenTofu variable keys", "de": "OpenTofu-Variablenschlüssel"},
    "log.tofu_var_large": {
        "en": "OpenTofu variable '{variable}' is {size_kb} KB encoded — close to the cloud-init user_data "
        "limit; the VM may fail to boot if the template inlines the full value.",
        "de": "OpenTofu-Variable '{variable}' ist kodiert {size_kb} KB groß — nahe am Limit für "
        "cloud-init-user_data; die VM startet möglicherweise nicht, wenn das Template den vollen Wert "
        "einbettet.",
    },
    "log.tofu_init_ok": {"en": "OpenTofu initialization completed", "de": "OpenTofu-Initialisierung abgeschlossen"},
    "log.tofu_init_failed": {"en": "OpenTofu init failed", "de": "OpenTofu-Initialisierung fehlgeschlagen"},
    "log.tofu_plan_ok": {
        "en": "OpenTofu plan completed successfully",
        "de": "OpenTofu-Plan erfolgreich abgeschlossen",
    },
    "log.tofu_plan_failed": {"en": "OpenTofu plan failed", "de": "OpenTofu-Plan fehlgeschlagen"},
    "log.tofu_apply_failed": {"en": "OpenTofu apply failed", "de": "OpenTofu-Apply fehlgeschlagen"},
    "log.tofu_created": {"en": "OpenTofu resources created", "de": "OpenTofu-Ressourcen erstellt"},
    "log.tofu_destroyed": {"en": "OpenTofu resources destroyed", "de": "OpenTofu-Ressourcen gelöscht"},
    "log.tofu_outputs_collected": {
        "en": "OpenTofu deployment outputs collected",
        "de": "OpenTofu-Ausgaben des Deployments gesammelt",
    },
    "log.tofu_output": {"en": "OpenTofu deployment output", "de": "OpenTofu-Ausgabe des Deployments"},
    "log.tofu_cleanup_running": {
        "en": "Running tofu destroy to clean up partially-applied resources",
        "de": "tofu destroy wird ausgeführt, um teilweise angewendete Ressourcen aufzuräumen",
    },
    "log.tofu_cleanup_failed": {
        "en": "OpenTofu cleanup failed: {error}",
        "de": "OpenTofu-Aufräumen fehlgeschlagen: {error}",
    },
    "log.destroy_stale_datasource": {
        "en": "Destroy blocked by a stale data source (image/network deleted out-of-band). Retrying with "
        "-refresh=false — resources are torn down from state.",
        "de": "Löschen durch veraltete Data Source blockiert (Image/Netzwerk außerhalb gelöscht). Neuer "
        "Versuch mit -refresh=false — die Ressourcen werden anhand des States abgebaut.",
    },
    "log.deployment_ok": {
        "en": "Deployment {deployment_id} completed successfully",
        "de": "Deployment {deployment_id} erfolgreich abgeschlossen",
    },
    "log.deployment_destroyed": {
        "en": "Deployment {deployment_id} destroyed successfully",
        "de": "Deployment {deployment_id} erfolgreich gelöscht",
    },
    "log.deployment_action_ok": {
        "en": "Deployment {deployment_id} {action}d successfully",
        "de": "Deployment {deployment_id}: {action} erfolgreich abgeschlossen",
    },
    "log.deployment_redeployed": {
        "en": "Deployment {deployment_id} redeploy of {resource_address} completed",
        "de": "Deployment {deployment_id}: Neu-Deployment von {resource_address} abgeschlossen",
    },
    "log.resource_replaced": {
        "en": "Resource {resource_address} replaced",
        "de": "Ressource {resource_address} ersetzt",
    },
    "log.deployment_summary": {"en": "Deployment summary", "de": "Zusammenfassung des Deployments"},
    "log.destroy_summary": {"en": "Destroy summary", "de": "Zusammenfassung des Löschvorgangs"},
    "log.deployment_failed": {
        "en": "Deployment failed: {error}",
        "de": "Deployment fehlgeschlagen: {error}",
    },
    "log.destroy_failed": {"en": "Destroy failed: {error}", "de": "Löschen fehlgeschlagen: {error}"},
    "log.redeploy_failed": {
        "en": "Redeploy failed: {error}",
        "de": "Neu-Deployment fehlgeschlagen: {error}",
    },
    "log.action_failed": {"en": "{action} failed: {error}", "de": "{action} fehlgeschlagen: {error}"},
    "log.instances_found": {
        "en": "{count} compute instance(s) found",
        "de": "{count} Compute-Instanz(en) gefunden",
    },
    "log.server_op": {
        "en": "{server_op} {label} (status: {status})",
        "de": "{server_op}: {label} (Status: {status})",
    },
    "log.server_op_ok": {"en": "{label}: {server_op} OK", "de": "{label}: {server_op} erfolgreich"},
    "log.server_op_failed": {
        "en": "{label}: {server_op} failed: {error}",
        "de": "{label}: {server_op} fehlgeschlagen: {error}",
    },
    "log.roster_dropped": {
        "en": "Redeploy roster reconciliation: dropped {count} orphan slot(s) from variable " "'{variable}': {dropped}",
        "de": "Abgleich der Teilnehmerliste beim Neu-Deployment: {count} verwaiste(r) Slot(s) aus Variable "
        "'{variable}' entfernt: {dropped}",
    },
    "log.roster_default": {
        "en": "Redeploy roster reconciliation: variable '{variable}' has no surviving slots after roster "
        "intersection — falling back to its declared default (or required-but-missing).",
        "de": "Abgleich der Teilnehmerliste beim Neu-Deployment: Variable '{variable}' hat nach dem Abgleich "
        "keine Slots mehr — es wird der deklarierte Standardwert verwendet (bzw. 'erforderlich, aber "
        "fehlt').",
    },
    # ------------------------------------------------------------------
    # Fehlermeldungen (raise ...), landen als ``Failure`` beim Backend
    # ------------------------------------------------------------------
    "error.envelope_missing_deploy": {
        "en": "OpenStack credential envelope missing — user must upload credentials before deploying",
        "de": "OpenStack-Zugangsdaten fehlen — die Nutzer*in muss vor dem Deployment Zugangsdaten hochladen",
    },
    "error.envelope_missing_action": {
        "en": "OpenStack credential envelope missing — cannot {action} without credentials",
        "de": "OpenStack-Zugangsdaten fehlen — {action} ist ohne Zugangsdaten nicht möglich",
    },
    "error.envelope_missing_destroy": {
        "en": "OpenStack credential envelope missing — cannot destroy without credentials",
        "de": "OpenStack-Zugangsdaten fehlen — Löschen ist ohne Zugangsdaten nicht möglich",
    },
    "error.envelope_missing_redeploy": {
        "en": "OpenStack credential envelope missing — cannot redeploy without credentials",
        "de": "OpenStack-Zugangsdaten fehlen — Neu-Deployment ist ohne Zugangsdaten nicht möglich",
    },
    "error.git_clone_failed": {"en": "Git clone failed: {error}", "de": "Git-Clone fehlgeschlagen: {error}"},
    "error.git_command_failed": {
        "en": "Git command failed: {error}",
        "de": "Git-Befehl fehlgeschlagen: {error}",
    },
    "error.git_release_clone_failed": {
        "en": "Failed to clone release {tag} from {git_url}: {error}",
        "de": "Release {tag} konnte nicht aus {git_url} geklont werden: {error}",
    },
    "error.packer_discovery_failed": {
        "en": "Packer template discovery failed: {error}",
        "de": "Suche nach Packer-Templates fehlgeschlagen: {error}",
    },
    "error.packer_error": {"en": "Packer error: {error}", "de": "Packer-Fehler: {error}"},
    "error.packer_init_failed": {
        "en": "{log_prefix}Packer init failed",
        "de": "{log_prefix}Packer-Initialisierung fehlgeschlagen",
    },
    "error.packer_validation_failed": {
        "en": "{log_prefix}Packer validation failed: {error}",
        "de": "{log_prefix}Packer-Validierung fehlgeschlagen: {error}",
    },
    "error.packer_build_failed": {
        "en": "{log_prefix}Packer build failed: {error}",
        "de": "{log_prefix}Packer-Build fehlgeschlagen: {error}",
    },
    "error.tofu_error": {"en": "OpenTofu error: {error}", "de": "OpenTofu-Fehler: {error}"},
    "error.tofu_dir_missing": {
        "en": "Terraform directory not found at {path}",
        "de": "Terraform-Verzeichnis nicht gefunden unter {path}",
    },
    "error.tofu_destroy_failed": {"en": "OpenTofu destroy failed", "de": "OpenTofu-Destroy fehlgeschlagen"},
    "error.tofu_replace_failed": {
        "en": "OpenTofu apply (replace) failed",
        "de": "OpenTofu-Apply (Austausch) fehlgeschlagen",
    },
    "error.no_instances": {
        "en": "No compute instances found in OpenTofu state — nothing to {action}. The deployment may have "
        "been torn down already or never reached a successful apply.",
        "de": "Keine Compute-Instanzen im OpenTofu-State gefunden — nichts zu tun für: {action}. Das "
        "Deployment wurde möglicherweise bereits abgebaut oder hat nie ein erfolgreiches Apply erreicht.",
    },
    "error.servers_failed": {
        "en": "{action} failed for {failed}/{total} server(s): {joined}",
        "de": "{action} fehlgeschlagen für {failed}/{total} Server: {joined}",
    },
    "error.invalid_resource_address": {
        "en": "redeploy_resource called with invalid resource_address: {resource_address}",
        "de": "redeploy_resource wurde mit ungültiger resource_address aufgerufen: {resource_address}",
    },
    # Rückgabewerte der Services, die in Log-Zeilen und ``Failure``-Texten landen
    "error.cli_timeout": {
        "en": "Timeout after {seconds}s running: {command}",
        "de": "Zeitüberschreitung nach {seconds}s bei: {command}",
    },
    "error.cli_not_found": {
        "en": "OpenStack CLI not found — install python-openstackclient",
        "de": "OpenStack-CLI nicht gefunden — python-openstackclient installieren",
    },
    "error.cli_failed": {
        "en": "Error running openstack CLI: {error}",
        "de": "Fehler beim Ausführen der OpenStack-CLI: {error}",
    },
    "error.server_stop_failed": {
        "en": "openstack server stop failed",
        "de": "openstack server stop fehlgeschlagen",
    },
    "error.server_start_failed": {
        "en": "openstack server start failed",
        "de": "openstack server start fehlgeschlagen",
    },
    "error.timeout": {"en": "Timeout", "de": "Zeitüberschreitung"},
    "error.unknown": {"en": "Unknown error", "de": "Unbekannter Fehler"},
    "error.unknown_lower": {"en": "unknown error", "de": "unbekannter Fehler"},
    "error.envelope_invalid": {
        "en": "OpenStack credential envelope is missing or invalid",
        "de": "OpenStack-Zugangsdaten fehlen oder sind ungültig",
    },
    "error.envelope_field": {
        "en": "OpenStack credential envelope missing field: {field}",
        "de": "In den OpenStack-Zugangsdaten fehlt das Feld: {field}",
    },
    "error.envelope_decrypt": {
        "en": "Failed to decrypt OpenStack credential envelope ({error_type}). Check that backend and worker "
        "share the same CREDENTIAL_ENCRYPTION_KEY.",
        "de": "OpenStack-Zugangsdaten konnten nicht entschlüsselt werden ({error_type}). Bitte prüfen, ob "
        "Backend und Worker denselben CREDENTIAL_ENCRYPTION_KEY verwenden.",
    },
    "error.credentials_shredded": {
        "en": "Credentials already shredded",
        "de": "Zugangsdaten wurden bereits vernichtet",
    },
    "error.build_lock_timeout": {
        "en": "Timed out waiting for Packer build lock {key} (another worker is still building this image)",
        "de": "Zeitüberschreitung beim Warten auf die Packer-Build-Sperre {key} (ein anderer Worker baut "
        "dieses Image noch)",
    },
    "error.packer_invalid_keys": {
        "en": "Packer template subdirectories with invalid keys (must match [a-z][a-z0-9_-]{{0,30}}): {keys}",
        "de": "Packer-Template-Unterverzeichnisse mit ungültigen Schlüsseln (erlaubt: "
        "[a-z][a-z0-9_-]{{0,30}}): {keys}",
    },
    "error.packer_mixed_layout": {
        "en": "App repository has BOTH packer/template.pkr.hcl (legacy layout) AND "
        "packer/<key>/template.pkr.hcl subdirectories ({keys}). Choose one layout — remove the legacy "
        "file or the subdirectories.",
        "de": "Das App-Repository enthält SOWOHL packer/template.pkr.hcl (altes Layout) ALS AUCH "
        "Unterverzeichnisse packer/<key>/template.pkr.hcl ({keys}). Bitte ein Layout wählen — entweder die "
        "alte Datei oder die Unterverzeichnisse entfernen.",
    },
}
