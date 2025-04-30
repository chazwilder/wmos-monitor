"""
Code analyzer module for normalizing and comparing database object source code
"""

import re
import hashlib
import difflib
import os

from utils.logger import logger


class CodeAnalyzer:
    """Analyzes, normalizes, and compares database object source code"""

    @staticmethod
    def normalize_code(code):
        """
        Normalize code to avoid false positives in change detection

        Args:
            code (str): Raw source code

        Returns:
            tuple: (clean_code, normalized_code) - clean for diffing, normalized for hashing
        """
        if code is None:
            return "", ""

        code = code.replace("\r\n", "\n")  # Normalize line endings

        # Preserve original code for diffing
        clean_code = code

        # Create a normalized version for hashing
        # Remove SQL comments (both -- and /* */ style)
        normalized = re.sub(r"/\*[\s\S]*?\*/", " ", code)
        normalized = re.sub(r"--.*$", " ", normalized, flags=re.MULTILINE)

        # Check for common SQL object definitions and ensure consistent prefixes
        obj_types = ["PROCEDURE", "FUNCTION", "TRIGGER", "VIEW", "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY"]
        for obj_type in obj_types:
            # If the code contains the object type but doesn't have CREATE OR REPLACE prefix
            pattern = rf"^\s*({obj_type}\s+\w+)"
            if re.search(pattern, normalized, re.IGNORECASE | re.MULTILINE):
                if not re.search(rf"CREATE\s+OR\s+REPLACE\s+{obj_type}", normalized, re.IGNORECASE):
                    # Add the prefix for consistent normalization
                    normalized = re.sub(pattern, f"CREATE OR REPLACE \\1", normalized, flags=re.IGNORECASE | re.MULTILINE)

            if re.search(pattern, clean_code, re.IGNORECASE | re.MULTILINE):
                if not re.search(rf"CREATE\s+OR\s+REPLACE\s+{obj_type}", clean_code, re.IGNORECASE):
                    # Add the prefix for consistent normalization
                    normalized = re.sub(pattern, f"CREATE OR REPLACE \\1", clean_code, flags=re.IGNORECASE | re.MULTILINE)

        normalized = " ".join(normalized.split())
        normalized = normalized.upper()
        normalized = re.sub(r'"[A-Z0-9_]+"\.(".*?")', r"\1", normalized)
        normalized = re.sub(r"\s+$", "", normalized)

        return clean_code, normalized

    @staticmethod
    def hash_code(normalized_code):
        """
        Create a hash of the normalized code

        Args:
            normalized_code (str): Normalized code

        Returns:
            str: MD5 hash of the normalized code
        """
        return hashlib.md5(normalized_code.encode()).hexdigest()

    @staticmethod
    def generate_diff(old_code, new_code):
        """
        Generate a diff between two versions of code

        Args:
            old_code (str): Previous version of the code
            new_code (str): Current version of the code

        Returns:
            tuple: (diff_text, changed_lines) - diff in unified format and count of changed lines
        """
        if old_code is None:
            old_code = ""
        if new_code is None:
            new_code = ""

        old_lines = old_code.splitlines()
        new_lines = new_code.splitlines()

        diff = difflib.unified_diff(
            old_lines, new_lines,
            fromfile="previous",
            tofile="current",
            lineterm=""
        )

        diff_list = list(diff)
        changed_lines = sum(
            1 for line in diff_list if line.startswith("+") or line.startswith("-")
        )

        diff_text = "\n".join(diff_list)

        return diff_text, changed_lines

    @staticmethod
    def save_to_filesystem(base_dir, schema, object_name, object_type, source_code):
        """
        Save raw source code to filesystem with directory pattern and ensure CREATE OR REPLACE prefix

        Args:
            base_dir (str): Base directory to save files
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type
            source_code (str): Source code to save

        Returns:
            str: Path to saved file or None if source_code is empty
        """
        if not source_code:
            return None

        # Add CREATE OR REPLACE prefix if needed
        if object_type in ["PROCEDURE", "FUNCTION", "TRIGGER", "VIEW", "PACKAGE", "PACKAGE BODY", "TYPE", "TYPE BODY"]:
            # Check if it already has CREATE OR REPLACE
            if not re.search(rf"CREATE\s+OR\s+REPLACE\s+{object_type}", source_code, re.IGNORECASE):
                # Add the prefix
                pattern = rf"^\s*({object_type}\s+\w+)"
                source_code = re.sub(pattern, f"CREATE OR REPLACE \\1", source_code, flags=re.IGNORECASE | re.MULTILINE)

        schema_dir = os.path.join(base_dir, schema)
        os.makedirs(schema_dir, exist_ok=True)

        simple_type = object_type.replace(" ", "_").upper()
        type_dir = os.path.join(schema_dir, simple_type)
        os.makedirs(type_dir, exist_ok=True)

        file_path = os.path.join(type_dir, f"{object_name}.sql")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(source_code)

        logger.info(f"Saved source code to {file_path}")
        return file_path