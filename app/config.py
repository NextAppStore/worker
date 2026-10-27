from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # The AppStore database, which is also the job queue: the worker claims
    # tasks from ``tasks``, appends ``task_events`` and records results.
    # Connect as ``appstore_worker``: the backend migration grants that role
    # exactly the columns in ``app/job_tables.py`` and nothing else.
    DATABASE_URL: str

    # Jobs run in parallel per worker process. Each is mostly waiting on
    # OpenTofu, Packer or OpenStack, so threads are enough.
    WORKER_CONCURRENCY: int = 4
    # A claimed task belongs to this worker until the lease runs out; it is
    # renewed every third of it while the job runs. When a worker dies, the
    # API fails its tasks once their lease has expired. Nothing is retried.
    WORKER_LEASE_SECONDS: int = 120
    # A job taking longer is stopped: its running tool is killed and the task
    # fails. Covers a Packer build (≤ 1 h) plus an apply with room to spare.
    WORKER_JOB_TIMEOUT_SECONDS: int = 7200
    # When set, worker slot n runs its jobs' tools as UID/GID base+n (see
    # job_context.py), so app code cannot read the worker's environment or a
    # neighbouring job's files. Requires the worker to run as root; the
    # image sets it.
    WORKER_JOB_UID_BASE: int | None = None
    # Per-slot home directories of those users (plugin caches).
    WORKER_JOB_HOME_BASE: str = "/var/lib/appstore-worker"
    # Liveness for Docker's HEALTHCHECK: every WORKER_HEALTH_INTERVAL_SECONDS
    # the worker touches this file, as long as all its slots run and the
    # database answers. The image's HEALTHCHECK fails when it gets stale.
    WORKER_HEALTH_FILE: str = "/tmp/appstore-worker.healthy"
    WORKER_HEALTH_INTERVAL_SECONDS: int = 30

    # The backend API as the worker's jobs reach it on the container
    # network; OpenTofu keeps each deployment's state there
    # (``/internal/tfstate``), e.g. http://backend:8000.
    APPSTORE_API_URL: str = "http://backend:8000"

    # OpenTofu's http backend retries a failed state request (connection
    # refused, 5xx) this often, waiting up to WAIT_MAX seconds between tries
    # (exponential from 1 s). 10 × ≤30 s bridges ~3 minutes, e.g. the backend
    # being restarted by a deploy in the middle of an apply. OpenTofu's own
    # default is 2 retries, a few seconds.
    WORKER_STATE_RETRY_MAX: int = 10
    WORKER_STATE_RETRY_WAIT_MAX: int = 30
    # If OpenTofu still could not save the state, it leaves errored.tfstate
    # in the job directory. The worker keeps pushing it for this long (backoff
    # 5 s … 60 s) before giving up and keeping an encrypted copy in
    # WORKER_STATE_RESCUE_DIR (a volume), which the backend's
    # scripts/restore_tfstate.py puts back.
    WORKER_STATE_RESCUE_SECONDS: int = 900
    WORKER_STATE_RESCUE_DIR: str = "/var/lib/appstore-worker/rescued-state"

    # Where jobs clone app repositories.
    TEMP_REPO_BASE_PATH: str = "/tmp/worker_repos"

    # OpenTofu/Packer paths (installed in container)
    TOFU_PATH: str = "/usr/local/bin/tofu"
    PACKER_PATH: str = "/usr/local/bin/packer"
    # OpenTofu log level for debugging the worker (TF_LOG); off by default
    # because the trace drowns the error a user needs to see.
    WORKER_TF_LOG: str = ""

    # Symmetric Fernet key shared with the backend: opens the sealed job
    # payload and the OpenStack credential envelope inside it.
    CREDENTIAL_ENCRYPTION_KEY: str

    # Git
    GIT_ACCESS_TOKEN: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
