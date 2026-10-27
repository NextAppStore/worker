"""Git service for repository management with HTTPS token authentication."""

import logging
import re
import shutil
from pathlib import Path

import git

from ..config import settings

logger = logging.getLogger(__name__)


class GitService:
    """Service for Git operations with HTTPS token authentication."""

    def __init__(self) -> None:
        """Initialize Git service with settings."""
        self.base_path = Path(settings.TEMP_REPO_BASE_PATH)
        self.token = settings.GIT_ACCESS_TOKEN

    def _parse_git_url(self, git_url: str) -> dict | None:
        """Parse Git URL and return components."""
        # SSH format: git@github.com:owner/repo.git
        ssh_pattern = r"^git@([^:]+):([^/]+)/(.+?)(?:\.git)?$"
        # HTTPS format: https://github.com/owner/repo.git
        https_pattern = r"^https?://([^/]+)/([^/]+)/(.+?)(?:\.git)?$"

        ssh_match = re.match(ssh_pattern, git_url)
        if ssh_match:
            host, owner, repo = ssh_match.groups()
            return {"host": host, "owner": owner, "repo": repo}

        https_match = re.match(https_pattern, git_url)
        if https_match:
            host, owner, repo = https_match.groups()
            return {"host": host, "owner": owner, "repo": repo}

        return None

    def _get_authenticated_url(self, git_url: str) -> str:
        """Convert Git URL to HTTPS format with token authentication."""
        url = git_url
        if url.startswith("git@"):
            # Convert SSH to HTTPS: git@github.com:owner/repo.git -> https://token@github.com/owner/repo.git
            url = url.replace(":", "/").replace("git@", f"https://{self.token}@")
        elif url.startswith("https://"):
            # Add token to HTTPS URL
            parsed = self._parse_git_url(url)
            if parsed:
                url = f"https://{self.token}@{parsed['host']}/{parsed['owner']}/{parsed['repo']}.git"

        return url

    def clone_release(self, git_url: str, tag: str, deployment_id: str) -> str:
        """
        Clone a specific release/tag using shallow clone with HTTPS token authentication.

        Args:
            git_url: URL of the Git repository
            tag: Tag/Release name
            deployment_id: Unique ID for the target folder

        Returns:
            Path to the cloned repository

        Raises:
            Exception with detailed error message
        """
        repo_path = self.base_path / f"deploy_{deployment_id}"

        if repo_path.exists():
            logger.info(f"Removing existing repo at {repo_path}")
            shutil.rmtree(repo_path)

        try:
            # Convert to authenticated HTTPS URL
            auth_url = self._get_authenticated_url(git_url)

            logger.info(f"Cloning repository {git_url}")
            logger.info(f"Target: {repo_path}")
            logger.info(f"Branch/Tag: {tag}")
            logger.info("Mode: Shallow clone (depth=1, single branch)")

            # Clone with --branch <tag> --depth 1 (no history, only the tag)
            repo = git.Repo.clone_from(auth_url, str(repo_path), branch=tag, depth=1, single_branch=True)
            # The clone stored the token-bearing URL in .git/config. The
            # app's own code (OpenTofu local-exec, Packer provisioners) runs
            # in this directory, so put the plain URL back.
            repo.remotes.origin.set_url(git_url)

            logger.info(f"✓ Repository cloned successfully to {repo_path}")
            logger.info(f"Current HEAD: {repo.head.commit.hexsha}")

            return str(repo_path)

        except git.exc.GitCommandError as e:
            error_msg = f"Git command failed: {e.stderr if hasattr(e, 'stderr') else str(e)}"
            logger.error(error_msg)
            if repo_path.exists():
                shutil.rmtree(repo_path)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Failed to clone release {tag} from {git_url}: {str(e)}"
            logger.error(error_msg)
            if repo_path.exists():
                shutil.rmtree(repo_path)
            raise Exception(error_msg)

    def cleanup_repository(self, repo_path: str) -> None:
        """Delete the cloned repository."""
        path = Path(repo_path)
        if path.exists():
            logger.info(f"Cleaning up {repo_path}")
            shutil.rmtree(path)


# Singleton
git_service = GitService()
