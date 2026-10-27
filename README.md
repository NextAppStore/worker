# Worker

[![Coverage](https://img.shields.io/endpoint?url=https://six7-click-n-deploy.github.io/worker/badge.json)](https://six7-click-n-deploy.github.io/worker/)

Worker des App Stores. Holt Deployment-Tasks aus der Postgres-Queue (Tabelle `tasks` des Backends), klont das App-Repository, führt Packer + OpenTofu aus und provisioniert auf OpenStack.

## Setup

Dieses Repository wird nicht eigenständig gestartet. Der Worker braucht die Datenbank des Backends (als Rolle `appstore_worker`) und das Backend selbst (OpenTofu-State unter `/internal/tfstate`) — der gesamte Stack wird über das deployment-Repository hochgefahren. Vollständige Anleitung: [deployment/README.md](https://github.com/six7-click-n-deploy/deployment#readme).

Voraussetzung für alle folgenden Befehle: `make dev-up` aus dem `deployment/`-Verzeichnis wurde ausgeführt und der Stack läuft.

## Entwicklung

Alle `make`-Befehle werden aus dem `deployment/`-Verzeichnis des [deployment-Repos](https://github.com/six7-click-n-deploy/deployment) ausgeführt — dort liegt das Makefile.

```bash
# in app-store/deployment
make dev-restart-worker   # Worker neu starten (z. B. nach Änderung an tasks.py)
make dev-logs-worker      # Worker-Logs verfolgen
make shell-worker         # interaktive Shell im Container
```

Tests, Lint und Format laufen im Worker-Container — `make shell-worker` öffnet eine Shell, in der `poetry run pytest`, `poetry run ruff check` und `poetry run ruff format` zur Verfügung stehen.

## Was der Worker tut

- **Deploy**: klont das App-Repo am Release-Tag, baut bei Bedarf ein Packer-Image, führt `tofu apply` aus
- **Destroy**: `tofu destroy` gegen denselben Tag/dieselben Variablen
- **Update**: deployt neue Version im Bestands-State
- **OpenStack-Auth**: per-Task `clouds.yaml`, generiert aus dem vom Backend verschlüsselten Credentials-Envelope
- **Isolation**: Tools laufen mit minimaler Umgebung (keine Worker-Secrets), unter eigener UID pro Slot und mit Zeitlimit (`job_context.py`)

## Ablauf

`python -m app` startet `WORKER_CONCURRENCY` Slots. Jeder Slot hört per `LISTEN task_queued`, arbeitet den Rückstau mit `SELECT … FOR UPDATE SKIP LOCKED` ab und wartet dann blockierend auf den nächsten Weckruf — es gibt kein Polling. Während ein Job läuft, erneuert der Worker seine Lease; Fortschritt und Log-Zeilen landen gebündelt in `task_events`, das Ergebnis mit Terminal-Event auf der Task-Zeile. Stirbt der Worker, läuft die Lease ab und das Backend beendet den Task; **nichts wird automatisch wiederholt**. Bei SIGTERM nimmt er keine neuen Jobs an und lässt laufende fertig werden.

Für den `HEALTHCHECK` des Images berührt ein eigener Thread alle `WORKER_HEALTH_INTERVAL_SECONDS` (30 s) die Datei `WORKER_HEALTH_FILE`, solange alle Slots laufen und die Datenbank antwortet. Ist sie älter als 90 s, meldet Docker den Container als `unhealthy`. Neu gestartet wird er dadurch nicht, `restart: unless-stopped` greift nur, wenn der Prozess endet.

Der OpenTofu-State liegt im Backend (HTTP-Backend `/internal/tfstate`). Ist das Backend beim Speichern kurz weg, wiederholt OpenTofu die Anfrage (`WORKER_STATE_RETRY_*`, etwa 3 Minuten). Bleibt es länger weg, sichert der Worker `errored.tfstate` sofort verschlüsselt im Volume `WORKER_STATE_RESCUE_DIR`, versucht bis zu `WORKER_STATE_RESCUE_SECONDS` weiter hochzuladen und löscht die Sicherung, sobald das klappt. Andernfalls nennen Task-Log und Worker-Start die Datei, und das Backend spielt sie mit `scripts/restore_tfstate.py` zurück. So geht kein State verloren, während die Ressourcen in OpenStack weiterleben.

Welche Spalten der Worker anfassen darf, steht in `app/job_tables.py`. Das ist der Vertrag mit dem Backend (dessen Migration `f8a9b0c1d2e3_worker_role` vergibt genau diese Rechte, `tests/test_worker_contract.py` dort prüft sie). Spalten ändern heißt: beide Repos, Worker zuerst.

## Technologie-Stack

- **Postgres als Queue** (psycopg 3, `LISTEN/NOTIFY`, `SKIP LOCKED`, Advisory Locks)
- **OpenTofu 1.x** mit HTTP-State-Backend im Backend
- **Packer 1.x** für Image-Builds
- **GitPython** für Repo-Klone
- **SQLAlchemy 2.0** (Core) nur auf `tasks`/`task_events`
- **pytest** mit `unit` und `integration` als Markern

## Code-Struktur

Der Code liegt in `app/`. Einstieg ist `runner.py`: ein Slot holt einen Task und ruft die passende Job-Funktion in `tasks.py` auf, die die Services orchestriert — Repo klonen → (optional) Packer → OpenTofu → OpenStack. Jeder Service kapselt genau einen dieser Schritte.

```
app/
├── __main__.py      # python -m app → runner.main()
├── runner.py        # Slots: LISTEN task_queued, Claim, Job ausführen, Ergebnis speichern
├── job_queue.py     # Claim (SKIP LOCKED), Lease, Event-Senke, Ergebnis + Terminal-Event
├── job_tables.py    # Die Spalten von tasks/task_events, die der Worker sieht (Vertrag)
├── job_context.py   # Minimale Umgebung, UID pro Slot, Zeitlimit für die Tools
├── db.py            # Engine/Sessions (psycopg 3)
├── config.py        # Pydantic-Settings aus Env-Variablen
├── tasks.py         # Die Job-Funktionen + Failure-Exception; orchestriert die Services
├── services/        # Ein Service pro Deploy-Schritt (siehe unten)
└── utils/           # crypto (Envelope-Entschlüsselung), logger (strukturiertes Logging)
```

**tasks.py** definiert fünf Job-Funktionen (`runner.JOBS` ordnet sie den Task-Typen zu):

| Task | Zweck |
|---|---|
| `deploy_application` | Repo klonen → ggf. Packer-Image → `tofu apply` |
| `destroy_deployment` | `tofu destroy` gegen denselben State |
| `pause_deployment` | VMs stoppen (Daten bleiben) |
| `resume_deployment` | Pausierte VMs wieder starten |
| `redeploy_resource` | Einzelne Ressource im Bestands-State neu ausrollen |

Die `Failure`-Exception in `tasks.py` trägt Log, Fehler, Commit und Outputs eines fehlgeschlagenen Jobs; `job_queue.failure_columns` schreibt sie direkt auf die Task-Zeile.

**services/** — je ein Schritt der Pipeline:

| Service | Zweck |
|---|---|
| `git_service` | Klont das App-Repo am Release-Tag (HTTPS + Token) |
| `packer_discovery` | Findet Packer-Templates im geklonten Repo |
| `packer_executor` | Führt Packer-Builds aus (strukturiertes Logging) |
| `opentofu_executor` | Führt `tofu init/plan/apply/destroy` aus (HTTP-State-Backend im Backend) |
| `openstack_auth` | Materialisiert per-Task `clouds.yaml` aus dem verschlüsselten Credentials-Envelope |
| `openstack_service` | OpenStack-Operationen, v.a. Image-Management |
| `build_lock` | Postgres-Advisory-Lock um den Packer-Build (blockierendes Warten, frei sobald der Halter stirbt) |

## Mehr

- Architektur und projektübergreifende Doku: [.github-Repo](https://github.com/six7-click-n-deploy/.github)
- Backend-Service: [backend-Repo](https://github.com/six7-click-n-deploy/backend)

## IPv4-/IPv6-Datenfluss testen

`tests/test_ipv6_payloads.py` prüft IPv4/IPv6-Adressen, CIDRs und URLs an den
Schnittstellen Job-Payload, Ergebnisspalten, Packer und OpenTofu. Der Integrationstest führt mit
dem im Worker-Container installierten OpenTofu ein lokales Apply ohne
Provider und ohne Cloud-Ressourcen aus. Außerhalb des Containers wird dieser
Test bei fehlender OpenTofu-Binary übersprungen; das ersetzt keinen
Funktionsnachweis. Für die Abnahme im Worker-Container ausführen:

```bash
poetry run pytest tests/test_ipv6_payloads.py -v
```

Die Netzwerk-Erreichbarkeit der Cloud und ein echter Packer-Image-Build sind
separate Deployment-/App-Template-Tests.
