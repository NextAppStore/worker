# Worker

[![Coverage](https://img.shields.io/endpoint?url=https://six7-click-n-deploy.github.io/worker/badge.json)](https://six7-click-n-deploy.github.io/worker/)

Celery-Worker des App Stores. Konsumiert Deployment-Tasks aus RabbitMQ, klont das App-Repository, führt Packer + Terraform aus und provisioniert auf OpenStack.

## Setup

Dieses Repository wird nicht eigenständig gestartet. Der Worker braucht RabbitMQ, Redis, die Postgres-tfstate-DB und vom Backend dispatchte Tasks — der gesamte Stack wird über das deployment-Repository hochgefahren. Vollständige Anleitung: [deployment/README.md](https://github.com/six7-click-n-deploy/deployment#readme).

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

- **Deploy**: klont das App-Repo am Release-Tag, baut bei Bedarf ein Packer-Image, führt `terraform apply` aus
- **Destroy**: `terraform destroy` gegen denselben Tag/dieselben Variablen
- **Update**: deployt neue Version im Bestands-State
- **OpenStack-Auth**: per-Task `clouds.yaml`, generiert aus dem vom Backend verschlüsselten Credentials-Envelope

## Technologie-Stack

- **Celery 5** mit RabbitMQ als Broker, Redis als Result-Backend
- **Terraform 1.x** mit Postgres-Remote-State
- **Packer 1.x** für Image-Builds
- **GitPython** für Repo-Klone
- **SQLAlchemy 2.0** nur lesend gegen die App-DB
- **pytest** mit `unit` und `integration` als Markern

## Code-Struktur

Der Code liegt in `app/`. Einstieg ist `tasks.py`: Celery ruft eine Task-Funktion auf, die die Services orchestriert — Repo klonen → (optional) Packer → Terraform → OpenStack. Jeder Service kapselt genau einen dieser Schritte.

```
app/
├── celery_app.py    # Celery-Instanz + Config (Broker, Result-Backend, Serializer)
├── config.py        # Pydantic-Settings aus Env-Variablen
├── tasks.py         # Die Celery-Tasks + Failure-Exception; orchestriert die Services
├── services/        # Ein Service pro Deploy-Schritt (siehe unten)
└── utils/           # crypto (Envelope-Entschlüsselung), logger (strukturiertes Logging)
```

**tasks.py** definiert fünf Celery-Tasks — jeder orchestriert die Services für seinen Ablauf:

| Task | Zweck |
|---|---|
| `tasks.deploy_application` | Repo klonen → ggf. Packer-Image → `terraform apply` |
| `tasks.destroy_deployment` | `terraform destroy` gegen denselben State |
| `tasks.pause_deployment` | VMs stoppen (Daten bleiben) |
| `tasks.resume_deployment` | Pausierte VMs wieder starten |
| `tasks.redeploy_resource` | Einzelne Ressource im Bestands-State neu ausrollen |

Die `Failure`-Exception in `tasks.py` trägt strukturierte Fehlerdaten durch Celery zurück, damit das Backend sie dem User anzeigen kann.

**services/** — je ein Schritt der Pipeline:

| Service | Zweck |
|---|---|
| `git_service` | Klont das App-Repo am Release-Tag (HTTPS + Token) |
| `packer_discovery` | Findet Packer-Templates im geklonten Repo |
| `packer_executor` | Führt Packer-Builds aus (strukturiertes Logging) |
| `terraform_executor` | Führt `terraform init/plan/apply/destroy` aus (Postgres-Remote-State) |
| `openstack_auth` | Materialisiert per-Task `clouds.yaml` aus dem verschlüsselten Credentials-Envelope |
| `openstack_service` | OpenStack-Operationen, v.a. Image-Management |
| `build_lock` | Redis-Lock um den Packer-Build, damit parallele Tasks nicht dasselbe Image doppelt bauen |

## Mehr

- Architektur und projektübergreifende Doku: [.github-Repo](https://github.com/six7-click-n-deploy/.github)
- Backend-Service: [backend-Repo](https://github.com/six7-click-n-deploy/backend)

## IPv4-/IPv6-Datenfluss testen

`tests/test_ipv6_payloads.py` prüft IPv4/IPv6-Adressen, CIDRs und URLs an den
Celery-, Packer- und Terraform-Schnittstellen. Der Integrationstest führt mit
dem im Worker-Container installierten Terraform ein lokales Apply ohne
Provider und ohne Cloud-Ressourcen aus. Außerhalb des Containers wird dieser
Test bei fehlender Terraform-Binary übersprungen; das ersetzt keinen
Funktionsnachweis. Für die Abnahme im Worker-Container ausführen:

```bash
poetry run pytest tests/test_ipv6_payloads.py -v
```

Die Netzwerk-Erreichbarkeit der Cloud und ein echter Packer-Image-Build sind
separate Deployment-/App-Template-Tests.
