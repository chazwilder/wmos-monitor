"""
SQLite storage module for tracking object history, changes, and source code
"""

import sqlite3
import datetime
from wmos_tracker.utils.logger import logger


class SQLiteStore:
    """Persistent storage for object state, changes, and source code using SQLite"""

    def __init__(self, db_file):
        """
        Initialize the SQLite store

        Args:
            db_file (str): Path to SQLite database file
        """
        self.db_file = db_file
        self.db_conn = None
        self._initialize_db()

    def _initialize_db(self):
        """Initialize the SQLite database and create tables if they don't exist"""
        self.db_conn = sqlite3.connect(self.db_file)
        cursor = self.db_conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS object_state (
                schema TEXT,
                object_name TEXT,
                object_type TEXT,
                hash TEXT,
                last_modified TEXT,
                capture_date TEXT,
                file_path TEXT,
                PRIMARY KEY (schema, object_name, object_type, capture_date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS object_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schema TEXT,
                object_name TEXT,
                object_type TEXT,
                change_date TEXT,
                old_hash TEXT,
                new_hash TEXT,
                diff_summary TEXT,
                changed_lines INTEGER,
                reviewed INTEGER DEFAULT 0,
                reviewer TEXT,
                review_date TEXT,
                devops_ticket TEXT,
                file_path TEXT,
                notified INTEGER DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS object_source (
                hash TEXT PRIMARY KEY,
                source_code TEXT
            )
        """)

        # Add git_commit_sha column if it doesn't exist
        try:
            cursor.execute("ALTER TABLE object_changes ADD COLUMN git_commit_sha TEXT")
            logger.info("Added git_commit_sha column to object_changes table")
        except sqlite3.OperationalError:
            pass

        # Add notified column if it doesn't exist
        try:
            cursor.execute("ALTER TABLE object_changes ADD COLUMN notified INTEGER DEFAULT 0")
            logger.info("Added notified column to object_changes table")
        except sqlite3.OperationalError:
            pass

        self.db_conn.commit()
        logger.info(f"Initialized SQLite database at {self.db_file}")

    def close(self):
        """Close the database connection"""
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
            logger.info("Closed SQLite database connection")

    def get_previous_state(self, schema, object_name, object_type):
        """
        Get the most recent previous state of an object

        Args:
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type

        Returns:
            dict: Previous state or None if not found
        """
        cursor = self.db_conn.cursor()

        query = """
            SELECT hash, capture_date, last_modified, file_path
            FROM object_state
            WHERE schema = ? AND object_name = ? AND object_type = ?
            ORDER BY capture_date DESC
            LIMIT 1
        """

        cursor.execute(query, (schema, object_name, object_type))
        result = cursor.fetchone()

        if result:
            cursor.execute("SELECT source_code FROM object_source WHERE hash = ?", (result[0],))
            source_row = cursor.fetchone()
            source_code = source_row[0] if source_row else None

            return {
                "hash": result[0],
                "capture_date": result[1],
                "last_modified": result[2],
                "file_path": result[3],
                "source_code": source_code,
            }
        else:
            return None

    def store_object_state(self, schema, object_name, object_type, code_hash, last_modified, file_path):
        """
        Store the current state of an object

        Args:
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type
            code_hash (str): Hash of normalized code
            last_modified (str): Last modification timestamp
            file_path (str): Path to saved file
        """
        cursor = self.db_conn.cursor()
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        query = """
            INSERT OR REPLACE INTO object_state
            (schema, object_name, object_type, hash, last_modified, capture_date, file_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(
            query,
            (schema, object_name, object_type, code_hash, last_modified, today, file_path)
        )

        self.db_conn.commit()

    def store_source_code(self, code_hash, source_code):
        """
        Store source code by hash

        Args:
            code_hash (str): Hash of normalized code
            source_code (str): Source code to store
        """
        if not source_code:
            return

        cursor = self.db_conn.cursor()

        cursor.execute("SELECT 1 FROM object_source WHERE hash = ?", (code_hash,))
        if not cursor.fetchone():
            # Store the source code
            cursor.execute(
                "INSERT INTO object_source (hash, source_code) VALUES (?, ?)",
                (code_hash, source_code)
            )
            self.db_conn.commit()

    def record_change(self, schema, object_name, object_type, old_hash, new_hash, diff_summary, changed_lines, file_path):
        """
        Record a change to an object

        Args:
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type
            old_hash (str): Previous code hash
            new_hash (str): New code hash
            diff_summary (str): Summary of changes
            changed_lines (int): Number of lines changed
            file_path (str): Path to saved file

        Returns:
            int: ID of the recorded change
        """
        cursor = self.db_conn.cursor()
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        query = """
            INSERT INTO object_changes
            (schema, object_name, object_type, change_date, old_hash, new_hash, 
             diff_summary, changed_lines, file_path, notified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """

        cursor.execute(
            query,
            (schema, object_name, object_type, today, old_hash, new_hash, diff_summary, changed_lines, file_path)
        )

        self.db_conn.commit()

        return cursor.lastrowid

    def update_change_with_commit(self, change_id, commit_sha):
        """
        Update a change record with the git commit SHA

        Args:
            change_id (int): ID of the change
            commit_sha (str): Git commit SHA
        """
        cursor = self.db_conn.cursor()

        query = """
            UPDATE object_changes
            SET git_commit_sha = ?
            WHERE id = ?
        """

        cursor.execute(query, (commit_sha, change_id))
        self.db_conn.commit()

    def get_unnotified_changes(self, hours=24):
        """
        Get all unnotified changes from the past specified hours

        Args:
            hours (int): Number of hours to look back

        Returns:
            list: List of dictionaries containing change data
        """
        cursor = self.db_conn.cursor()

        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=hours)
        cutoff_date = cutoff_time.strftime("%Y-%m-%d")

        query = """
            SELECT c.id, c.schema, c.object_name, c.object_type, c.change_date, 
                   c.changed_lines, c.file_path, c.git_commit_sha
            FROM object_changes c
            WHERE c.notified = 0
            AND c.change_date >= ?
            ORDER BY c.schema, c.object_type, c.object_name
        """

        cursor.execute(query, (cutoff_date,))

        changes = []
        for row in cursor:
            # Get the diff summary
            cursor2 = self.db_conn.cursor()
            cursor2.execute("SELECT diff_summary FROM object_changes WHERE id = ?", (row[0],))
            diff_row = cursor2.fetchone()
            diff_summary = diff_row[0] if diff_row else ""

            # Extract changed lines
            changed_lines_text = []
            for line in diff_summary.splitlines():
                if line.startswith("+") and not line.startswith("+++"):
                    changed_lines_text.append(line)

            changes.append({
                "id": row[0],
                "schema": row[1],
                "object_name": row[2],
                "object_type": row[3],
                "change_date": row[4],
                "changed_lines": row[5],
                "file_path": row[6],
                "git_commit_sha": row[7],
                "changed_content": "\n".join(changed_lines_text[:10]) +
                                   ("\n..." if len(changed_lines_text) > 10 else ""),
            })

        logger.info(f"Found {len(changes)} unnotified changes from the past {hours} hours")
        return changes

    def mark_changes_as_notified(self, change_ids):
        """
        Mark changes as notified

        Args:
            change_ids (list): List of change IDs to mark as notified
        """
        if not change_ids:
            return

        cursor = self.db_conn.cursor()

        batch_size = 100
        for i in range(0, len(change_ids), batch_size):
            batch = change_ids[i:i + batch_size]
            placeholders = ",".join(["?" for _ in batch])

            query = f"""
                UPDATE object_changes
                SET notified = 1
                WHERE id IN ({placeholders})
            """

            cursor.execute(query, batch)

        self.db_conn.commit()
        logger.info(f"Marked {len(change_ids)} changes as notified")