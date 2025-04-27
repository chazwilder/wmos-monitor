"""
Logging configuration for the WMOS tracker
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logger(log_file="wmos_tracker.log", log_level=logging.INFO, max_size_mb=5, backup_count=5):
    """
    Set up and configure the logger with rotation

    Args:
        log_file (str): Path to the log file
        log_level (int): Logging level (default: INFO)
        max_size_mb (int): Maximum size of each log file in megabytes
        backup_count (int): Number of backup files to keep

    Returns:
        logging.Logger: Configured logger
    """
    log_dir = Path("logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = log_dir / log_file

    max_bytes = max_size_mb * 1024 * 1024

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("WMOSTracker")
    logger.setLevel(log_level)

    if logger.handlers:
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Logger initialized with rotation (max_size: %sMB, backups: %s)", max_size_mb, backup_count)
    return logger

# Create a global logger instance
logger = setup_logger()