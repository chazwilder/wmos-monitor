"""
Code analyzer module for normalizing and comparing database object source code
"""

import re
import hashlib
import difflib
import os
from pathlib import Path

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

        clean_code = code

        # Create a normalized version for hashing
        # Remove SQL comments (both -- and /* */ style)
        normalized = re.sub(r"/\*[\s\S]*?\*/", " ", code)
        normalized = re.sub(r"--.*$", " ", normalized, flags=re.MULTILINE)
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
        Save raw source code to filesystem with directory pattern

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