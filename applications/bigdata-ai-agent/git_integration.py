"""
Git Integration
===============

This module provides basic version control operations for updating job
configuration files in a Git repository.  The agent writes
recommendation results to files under the repository and commits them
with an informative message.  Optionally it can push to a remote
repository if configured.

To configure, set ``git_repo_path`` (path to the local working copy) and
``git_remote_url`` (optional remote) on the Config object.  If the
repository is not initialised, ``GitHandler`` can initialise it and
optionally set the remote.  When pushing to a remote, authentication
must be configured via Git credential helpers (SSH keys or stored
credentials).  For security reasons the agent does not handle
credentials directly.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Iterable, Optional

from .config import Config

LOGGER = logging.getLogger(__name__)


class GitHandler:
    """Handle committing and optionally pushing changes to a Git repository."""

    def __init__(self, config: Config) -> None:
        self.repo_path = Path(config.git_repo_path) if config.git_repo_path else None
        self.remote_url = config.git_remote_url
        if self.repo_path:
            self.repo_path.mkdir(parents=True, exist_ok=True)

    def _run_git(self, args: Iterable[str]) -> subprocess.CompletedProcess:
        """Run a git command in the repository directory."""
        if not self.repo_path:
            raise ValueError("git_repo_path not configured")
        LOGGER.debug("Running git command: git %s", " ".join(args))
        return subprocess.run(
            ["git", *args],
            cwd=str(self.repo_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )

    def init_repo(self) -> None:
        """Initialise the repository if it does not already contain a .git directory."""
        if not self.repo_path:
            raise ValueError("git_repo_path not configured")
        if not (self.repo_path / ".git").exists():
            LOGGER.info("Initialising new git repository at %s", self.repo_path)
            self._run_git(["init"])
            if self.remote_url:
                LOGGER.info("Setting remote origin to %s", self.remote_url)
                self._run_git(["remote", "add", "origin", self.remote_url])

    def commit(self, files: Iterable[str], message: str) -> None:
        """Stage specified files and commit them with the given message."""
        if not self.repo_path:
            LOGGER.warning("git_repo_path not configured; skipping git commit")
            return
        # Stage files
        for f in files:
            rel = os.path.relpath(f, self.repo_path)
            LOGGER.debug("Staging file %s", rel)
            self._run_git(["add", rel])
        # Commit
        result = self._run_git(["commit", "-m", message])
        if result.returncode != 0:
            # If there are no changes, git will exit with code 1
            LOGGER.warning("Git commit returned non‑zero exit code: %s", result.stderr.strip())
        else:
            LOGGER.info("Committed changes: %s", message)

    def push(self, remote: str = "origin", branch: str = "main") -> None:
        """Push the current branch to the given remote if configured."""
        if not self.repo_path or not self.remote_url:
            LOGGER.warning("Remote URL not configured; skipping push")
            return
        result = self._run_git(["push", remote, branch])
        if result.returncode != 0:
            LOGGER.error("Git push failed: %s", result.stderr.strip())
        else:
            LOGGER.info("Pushed changes to %s/%s", remote, branch)
