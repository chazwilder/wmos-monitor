"""
Core tracking functionality for the WMOS database change tracker
"""

import os
import datetime

from database.oracle_client import OracleClient
from database.sqlite_store import SQLiteStore
from tracker.code_analyzer import CodeAnalyzer
from tracker.notifier import Notifier
from vcs.git_manager import GitManager
from utils.logger import logger


class ChangeTracker:
    """Main class for tracking database object changes"""

    def __init__(self, config):
        """
        Initialize the change tracker with configuration

        Args:
            config: Configuration object or dictionary
        """
        # Store configuration
        self.config = config

        # Initialize components
        self.oracle = OracleClient(config['CONNECTION_STRING'])
        self.store = SQLiteStore(config['DB_FILE'])
        self.git = GitManager(config['GIT_REPO_PATH'], config['DEVOPS_REPO_URL'])
        self.notifier = Notifier(config['POWER_AUTOMATE_WEBHOOK'], config['ENV_TYPE'])

        # Create directories
        os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
        os.makedirs(config['CODE_DIR'], exist_ok=True)

        # Set up timestamps
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        self.now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Initialized change tracker for {config['ENV_TYPE']} environment")

    def close(self):
        """Close all connections"""
        self.oracle.close()
        self.store.close()
        logger.info("Closed all connections")

    def scan_for_changes(self):
        """
        Scan all objects for changes since last capture

        Returns:
            dict: Results of the scan
        """
        # Connect to Oracle database
        if not self.oracle.connect():
            logger.error("Failed to connect to Oracle database")
            return None

        # Check if this is the first run
        is_first_run = self._is_first_run()

        # Find objects to scan
        objects = self.oracle.find_custom_objects(
            self.config['OBJECT_PREFIX'],
            self.config['DAYS_LOOKBACK'],
            is_first_run
        )

        logger.info(f"Scanning {len(objects)} objects for changes...")

        # If no objects found, return early
        if not objects:
            logger.info("No objects to scan")
            self.oracle.close()
            return {"total_objects": 0, "changed_objects": 0, "changes": []}

        change_count = 0
        changes = []
        git_changed_files = []

        # Process each object
        for obj in objects:
            schema = obj["schema"]
            object_name = obj["object_name"]
            object_type = obj["object_type"]
            last_modified = obj["last_modified"]

            # Process the object
            change = self._process_object(schema, object_name, object_type, last_modified)

            # If a change was detected
            if change:
                changes.append(change)
                git_changed_files.append(change["git_path"])
                change_count += 1

        logger.info(f"Scan complete. Found {change_count} changes.")

        # Commit to git if changes were found
        commit_sha = None
        if git_changed_files:
            commit_message = f"Update WMOS objects on {self.today} - {change_count} changes detected"
            commit_sha = self.git.commit_changes(git_changed_files, commit_message)

            # Update change records with commit SHA
            if commit_sha:
                for change in changes:
                    self.store.update_change_with_commit(change["id"], commit_sha)

        # Close Oracle connection
        self.oracle.close()

        return {
            "total_objects": len(objects),
            "changed_objects": change_count,
            "changes": changes,
            "git_commit": commit_sha,
        }

    def _is_first_run(self):
        """
        Check if this is the first run by examining the code directory

        Returns:
            bool: True if this is the first run, False otherwise
        """
        code_dir = self.config['CODE_DIR']
        return not os.path.exists(code_dir) or not os.listdir(code_dir)

    def _process_object(self, schema, object_name, object_type, last_modified):
        """
        Process a database object to detect and record changes

        Args:
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type
            last_modified (str): Last modification timestamp

        Returns:
            dict: Change information if a change was detected, None otherwise
        """
        # Fetch current source code
        current_source = self.oracle.fetch_object_source(schema, object_name, object_type)

        if not current_source:
            logger.warning(f"Could not fetch source for {schema}.{object_name} ({object_type})")
            return None

        # Clean for diffing and normalize for hashing
        clean_source, normalized_source = CodeAnalyzer.normalize_code(current_source)
        current_hash = CodeAnalyzer.hash_code(normalized_source)

        # Store source code
        self.store.store_source_code(current_hash, clean_source)

        # Get previous state
        prev_state = self.store.get_previous_state(schema, object_name, object_type)

        # Save to filesystem
        fs_path = CodeAnalyzer.save_to_filesystem(
            self.config['CODE_DIR'],
            schema,
            object_name,
            object_type,
            clean_source
        )

        # Save to git repo
        git_path = self.git.save_file(
            schema,
            object_name,
            object_type,
            clean_source
        )

        # Check if object is new or changed
        change = None
        if not prev_state or prev_state["hash"] != current_hash:
            if not prev_state:
                # New object
                logger.info(f"New object: {schema}.{object_name} ({object_type})")
                diff_text = f"New object created on {self.today}"
                changed_lines = len(clean_source.splitlines())
                old_hash = None
            else:
                # Changed object
                old_source = prev_state.get("source_code", "")

                # Generate diff
                diff_text, changed_lines = CodeAnalyzer.generate_diff(
                    old_source, clean_source
                )
                old_hash = prev_state["hash"]

                logger.info(f"Changed object: {schema}.{object_name} ({object_type}) - {changed_lines} lines changed")

            # Record the change
            change_id = self.store.record_change(
                schema,
                object_name,
                object_type,
                old_hash,
                current_hash,
                diff_text,
                changed_lines,
                fs_path
            )

            # Extract changed lines text
            changed_lines_text = []
            for line in diff_text.splitlines():
                if line.startswith("+") and not line.startswith("+++"):
                    changed_lines_text.append(line)

            # Create change object
            change = {
                "id": change_id,
                "schema": schema,
                "object_name": object_name,
                "object_type": object_type,
                "last_modified": last_modified,
                "changed_lines": changed_lines,
                "changed_content": "\n".join(changed_lines_text[:10]) +
                                   ("\n..." if len(changed_lines_text) > 10 else ""),
                "file_path": fs_path,
                "git_path": git_path,
            }

        # Always store current state
        self.store.store_object_state(
            schema,
            object_name,
            object_type,
            current_hash,
            last_modified,
            fs_path
        )

        return change

    def send_daily_summary(self):
        """
        Send a daily summary of changes from the past 24 hours

        Returns:
            bool: True if the notification was sent successfully, False otherwise
        """
        # Get all unnotified changes from the past 24 hours
        changes = self.store.get_unnotified_changes(hours=24)

        # Generate Azure DevOps link
        azure_devops_link = f"{self.config['DEVOPS_REPO_URL']}?_a=history"

        # Send notification
        result = self.notifier.send_daily_summary(changes, azure_devops_link)

        # Mark changes as notified if successful
        if result:
            self.store.mark_changes_as_notified([change["id"] for change in changes])

        return result

    def run_tracking_cycle(self):
        """
        Execute one complete tracking cycle

        Returns:
            dict: Results of the tracking cycle
        """
        logger.info("Starting database change tracking cycle")

        try:
            # Scan for changes
            logger.info("Scanning for database changes...")
            scan_results = self.scan_for_changes()

            if not scan_results:
                logger.error("Scan failed")
                return None

            logger.info(f"Scan complete: {scan_results['changed_objects']} changes found")

            # Check if it's time for the daily notification
            current_hour = datetime.datetime.now().hour
            notification_hour = self.config['DAILY_NOTIFICATION_HOUR']

            if current_hour == notification_hour:
                logger.info(f"Sending daily summary at {notification_hour}:00...")
                self.send_daily_summary()
            else:
                logger.info(f"Not sending notifications now. Daily summary will be sent at {notification_hour}:00")

            return scan_results

        except Exception as e:
            logger.error(f"Error in tracking cycle: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

        finally:
            # Close connections
            self.close()
            logger.info("Change tracking cycle complete")