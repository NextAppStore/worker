"""Tests for configuration."""

import os
from unittest.mock import patch

import pytest


@pytest.mark.unit
class TestConfiguration:
    """Test configuration loading."""

    def test_settings_import(self):
        """Test that settings can be imported."""
        from app.config import settings

        assert settings is not None

    def test_required_settings_exist(self):
        """Test that required settings attributes exist."""
        from app.config import settings

        assert hasattr(settings, "DATABASE_URL")
        assert hasattr(settings, "APPSTORE_API_URL")
        assert hasattr(settings, "WORKER_LEASE_SECONDS")
        assert hasattr(settings, "TEMP_REPO_BASE_PATH")
        assert hasattr(settings, "GIT_ACCESS_TOKEN")
        assert hasattr(settings, "CREDENTIAL_ENCRYPTION_KEY")

    @patch.dict(
        os.environ,
        {
            "CREDENTIAL_ENCRYPTION_KEY": "Q1PNlFd4It9oQPtjCcXcmB7wGDkY4w8KwpIRNSF4u7U=",
            "DATABASE_URL": "postgresql://appstore_worker:pw@postgres:5432/app",
            "WORKER_CONCURRENCY": "2",
            "GIT_ACCESS_TOKEN": "test-token",
        },
        clear=True,
    )
    def test_settings_from_environment(self):
        """Test loading settings from environment variables."""
        from importlib import reload

        from app import config

        reload(config)

        assert config.settings.DATABASE_URL.startswith("postgresql://appstore_worker")
        assert config.settings.WORKER_CONCURRENCY == 2
        assert config.settings.GIT_ACCESS_TOKEN == "test-token"
