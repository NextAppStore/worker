# =============================================================================
# Worker Production Dockerfile - Multi-Stage Build
# =============================================================================
# Optimiert für:
# - Multi-Platform (amd64 + arm64)
# - Kleines Image (kein Poetry im Runtime)
# - Reproduzierbare Builds
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder - Poetry installiert Dependencies
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Poetry installieren
ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

RUN pip install --no-cache-dir poetry

# Dependencies installieren (ohne Dev-Dependencies)
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --only=main --no-ansi

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Schlankes Production Image
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# Build arguments für Multi-Platform Support
ARG TARGETARCH
ARG OPENTOFU_VERSION=1.13.1
ARG PACKER_VERSION=1.15.3

WORKDIR /app

# System Dependencies. ``apt-get upgrade`` runs first so the base
# ``python:3.12-slim`` tag picks up Debian-security backports released
# after the upstream image was last rebuilt — that's where things like
# CVE-2026-45447 (openssl 3.5.6-1~deb13u2) come from. Trivy blocks the
# push on any HIGH/CRITICAL OS finding, so even though it enlarges the
# layer slightly we'd rather take the bytes than burn a .trivyignore
# line every time upstream debian releases a CVE-fix.
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    git \
    wget \
    unzip \
    curl \
    ca-certificates \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Upgrade base-image Python tooling (pip / setuptools / wheel) to pull
# in security fixes that the upstream `python:3.12-slim` tag hasn't
# picked up yet. Trivy scans these system site-packages — anything
# HIGH/CRITICAL here blocks the push.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# OpenStack CLI installieren
RUN pip install --no-cache-dir python-openstackclient

# OpenTofu installieren (platform-aware)
RUN ARCH="${TARGETARCH:-amd64}" && \
    echo "Installing OpenTofu ${OPENTOFU_VERSION} for ${ARCH}" && \
    wget -q https://github.com/opentofu/opentofu/releases/download/v${OPENTOFU_VERSION}/tofu_${OPENTOFU_VERSION}_linux_${ARCH}.zip && \
    unzip -qo tofu_${OPENTOFU_VERSION}_linux_${ARCH}.zip && \
    mv tofu /usr/local/bin/ && \
    rm -f tofu_${OPENTOFU_VERSION}_linux_${ARCH}.zip LICENSE CHANGELOG.md README.md && \
    tofu --version

# Packer installieren (platform-aware)
RUN ARCH="${TARGETARCH:-amd64}" && \
    echo "Installing Packer ${PACKER_VERSION} for ${ARCH}" && \
    wget -q https://releases.hashicorp.com/packer/${PACKER_VERSION}/packer_${PACKER_VERSION}_linux_${ARCH}.zip && \
    unzip -qo packer_${PACKER_VERSION}_linux_${ARCH}.zip && \
    mv packer /usr/local/bin/ && \
    rm -f packer_${PACKER_VERSION}_linux_${ARCH}.zip LICENSE.txt && \
    packer --version

# Virtual Environment vom Builder kopieren
COPY --from=builder /app/.venv /app/.venv

# Application Code kopieren
COPY app/ ./app/

# Job-Verzeichnisse (je Deployment, dem Slot-User übergeben) und die Homes
# der Slot-User: für die Slot-User betretbar, aber nicht auflistbar.
RUN mkdir -p /tmp/worker_repos /var/lib/appstore-worker && \
    chmod 0711 /tmp/worker_repos /var/lib/appstore-worker

# Environment für .venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WORKER_JOB_UID_BASE=20000

# Der Worker läuft als root (.trivyignore AVD-DS-0002) aus genau einem Grund:
# Er startet die Tools jedes Jobs als eigenen unprivilegierten User
# (UID 20000 + Slot, siehe app/job_context.py). App-Code (OpenTofu
# local-exec) ist nicht vertrauenswürdig; diese Trennung hält ihn von
# DATABASE_URL, dem Fernet-Key und dem Nachbar-Job fern.
#
# HEALTHCHECK: Der Worker berührt WORKER_HEALTH_FILE alle 30 s, solange
# alle Slots laufen und die Datenbank antwortet (app/runner.py). Ist die
# Datei älter als 90 s, ist der Container unhealthy. Lange Applies stören
# nicht, die Prüfung läuft in einem eigenen Thread. Ein hängender Job wird
# davon unabhängig über seine Lease vom Backend beendet.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import os, sys, time; sys.exit(time.time() - os.path.getmtime(os.environ.get('WORKER_HEALTH_FILE', '/tmp/appstore-worker.healthy')) > 90)"]

# Bei SIGTERM nimmt er keine neuen Jobs an und lässt laufende fertig
# laufen; die Compose-Dateien geben ihm dafür eine lange stop_grace_period.
CMD ["python", "-m", "app"]
