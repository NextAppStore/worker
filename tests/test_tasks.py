"""Smoke tests for the job functions."""

import inspect

import pytest


@pytest.mark.unit
class TestTaskBasics:
    """The jobs are plain functions taking their event sink first."""

    def test_task_import(self):
        """Test that the jobs can be imported."""
        from app.tasks import deploy_application

        assert deploy_application is not None

    def test_jobs_take_events_and_state_backend(self):
        """No Celery binding any more: events come in as the first argument."""
        from app import tasks

        for job in (
            tasks.deploy_application,
            tasks.destroy_deployment,
            tasks.pause_deployment,
            tasks.resume_deployment,
            tasks.redeploy_resource,
        ):
            params = list(inspect.signature(job).parameters)
            assert params[0] == "events"
            assert "state_backend" in params
