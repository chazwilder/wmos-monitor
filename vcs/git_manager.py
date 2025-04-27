"""
Git operations manager for handling version control
"""

import os
import re
import subprocess
import datetime
from pathlib import Path

from utils.logger import logger


class GitManager:
    """Manages Git operations for version control of database objects"""

    def __init__(self, repo_path, repo_url):
        """
        Initialize the Git manager

        Args:
            repo_path (str): Path to the Git repository
            repo_url (str): URL of the remote repository
        """
        self.repo_path = os.path.abspath(repo_path)
        self.repo_url = repo_url
        self.now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Ensure the Git repository exists
        self._init_repo()

    def _init_repo(self):
        """Initialize or update the Git repository"""
        if not os.path.exists(self.repo_path):
            logger.info(f"Cloning repository to {self.repo_path}")
            try:
                # Clone the repository
                subprocess.run(
                    ["git", "clone", self.repo_url, self.repo_path],
                    check=True,
                    capture_output=True,
                )
                logger.info("Repository cloned successfully")
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Failed to clone repository: {e.stderr.decode() if e.stderr else str(e)}"
                )
                # Create the directory and initialize if clone fails
                self._create_new_repo()
        else:
            logger.info(f"Using existing Git repository at {self.repo_path}")
            try:
                # Pull latest changes
                subprocess.run(
                    ["git", "pull", "origin", "main"],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )
                logger.info("Repository updated with latest changes")
            except subprocess.CalledProcessError as e:
                logger.warning(
                    f"Failed to pull latest changes: {e.stderr.decode() if e.stderr else str(e)}"
                )
                # Continue with the existing repo

    def _create_new_repo(self):
        """Create and initialize a new Git repository if cloning fails"""
        try:
            os.makedirs(self.repo_path, exist_ok=True)

            # Initialize the Git repository
            subprocess.run(
                ["git", "init"],
                cwd=self.repo_path,
                check=True,
                capture_output=True
            )

            # Create README.md
            readme_path = os.path.join(self.repo_path, "README.md")
            with open(readme_path, "w") as f:
                f.write("# WMOS Database Objects History\n\n")
                f.write("This repository contains tracked custom WMOS database objects.\n")
                f.write("Changes are automatically tracked by the WMOS Database Change Tracker.\n\n")
                f.write(f"Last updated: {self.now}\n")

            # Initial commit
            subprocess.run(
                ["git", "add", "README.md"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )

            # Configure Git user if not already set
            self._configure_git_user()

            subprocess.run(
                ["git", "commit", "-m", "Initial commit"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )

            # Add remote if URL provided
            if self.repo_url:
                subprocess.run(
                    ["git", "remote", "add", "origin", self.repo_url],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )

            logger.info("Git repository initialized successfully")
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Failed to initialize Git repository: {e.stderr.decode() if e.stderr else str(e)}"
            )

    def _configure_git_user(self):
        """Configure Git user for commits"""
        try:
            subprocess.run(
                ["git", "config", "user.name", "WMOS Database Tracker"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["git", "config", "user.email", "wmos.tracker@niagarawater.com"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )
            logger.info("Git user configured successfully")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to configure Git user: {e.stderr.decode() if e.stderr else str(e)}")

    def save_file(self, schema, object_name, object_type, source_code):
        """
        Save raw source code to git repository

        Args:
            schema (str): Schema name
            object_name (str): Object name
            object_type (str): Object type
            source_code (str): Source code to save

        Returns:
            str: Relative path of the saved file or None if source_code is empty
        """
        if not source_code:
            return None

        # Create schema directory
        schema_dir = os.path.join(self.repo_path, schema)
        os.makedirs(schema_dir, exist_ok=True)

        # Create object type directory
        simple_type = object_type.replace(" ", "_").upper()
        type_dir = os.path.join(schema_dir, simple_type)
        os.makedirs(type_dir, exist_ok=True)

        # Create file path
        file_path = os.path.join(type_dir, f"{object_name}.sql")

        # Save the file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(source_code)

        # Return the relative path from the repository root
        return os.path.relpath(file_path, self.repo_path)

    def update_readme(self):
        """Update the README.md file with current timestamp"""
        readme_path = os.path.join(self.repo_path, "README.md")

        if os.path.exists(readme_path):
            with open(readme_path, "r") as f:
                content = f.read()

            # Update the timestamp line
            if "Last updated:" in content:
                content = re.sub(r"Last updated: .*", f"Last updated: {self.now}", content)
            else:
                content += f"\n\nLast updated: {self.now}\n"
        else:
            content = "# WMOS Database Objects History\n\n"
            content += "This repository contains tracked custom WMOS database objects.\n"
            content += "Changes are automatically tracked by the WMOS Database Change Tracker.\n\n"
            content += f"Last updated: {self.now}\n"

        with open(readme_path, "w") as f:
            f.write(content)

        return "README.md"

    def commit_changes(self, changed_files, commit_message):
        """
        Commit changes to git repository and push to remote

        Args:
            changed_files (list): List of files to commit
            commit_message (str): Commit message

        Returns:
            str: Commit hash or None if no files were committed
        """
        if not changed_files:
            logger.info("No files to commit")
            return None

        try:
            # Update README with current timestamp
            readme_file = self.update_readme()

            # Add README first
            subprocess.run(
                ["git", "add", readme_file],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )

            # Add files in smaller batches to avoid command line length limits
            batch_size = 50
            for i in range(0, len(changed_files), batch_size):
                batch = changed_files[i:i + batch_size]
                subprocess.run(
                    ["git", "add"] + batch,
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                    )

            # Configure Git user if not already set
            self._configure_git_user()

            # Commit changes
            logger.info(f"Committing changes with message: {commit_message}")
            result = subprocess.run(
                ["git", "commit", "-m", commit_message],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )

            # Get the commit hash
            commit_hash = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

            logger.info(f"Committed changes to git repository: {commit_hash}")

            # Push to remote
            self._push_changes()

            return commit_hash

        except subprocess.CalledProcessError as e:
            logger.error(f"Git operation failed: {e.stderr.decode() if e.stderr else str(e)}")
            return None

    def _push_changes(self):
        """Push changes to remote repository with retry logic"""
        logger.info("Pushing changes to remote repository...")
        try:
            push_result = subprocess.run(
                ["git", "push", "origin", "main"],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )
            logger.info("Successfully pushed changes to remote repository")
        except subprocess.CalledProcessError as e:
            # If push fails because of diverging branches, try to pull and push again
            logger.warning(f"Push failed: {e.stderr.decode() if e.stderr else str(e)}")
            logger.info("Attempting to pull latest changes and push again...")

            try:
                # Pull with rebase
                pull_result = subprocess.run(
                    ["git", "pull", "--rebase", "origin", "main"],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )

                # Try pushing again
                push_retry = subprocess.run(
                    ["git", "push", "origin", "main"],
                    cwd=self.repo_path,
                    check=True,
                    capture_output=True,
                )
                logger.info("Successfully pushed changes after pull/rebase")
            except subprocess.CalledProcessError as e2:
                logger.error(f"Failed to push changes after pull/rebase: {e2.stderr.decode() if e2.stderr else str(e2)}")
                logger.error("Changes are committed locally but not pushed to remote repository")