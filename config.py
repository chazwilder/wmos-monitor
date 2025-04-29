"""
Configuration module for loading environment variables
"""

import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def load_config(env_file='.env.dev'):
    """
    Load configuration from environment variables, with .env.dev file support
    Returns a dictionary with all configuration values
    """
    load_dotenv(env_file)
    db_connection = os.getenv('CONNECTION_STRING', '')
    object_prefix = os.getenv('OBJECT_PREFIX', '')
    db_file = os.getenv('DB_FILE', 'wmos_changes.db')
    output_dir = os.getenv('OUTPUT_DIR', 'change_reports')
    webhook_url = os.getenv('POWER_AUTOMATE_WEBHOOK', '')
    code_dir = os.getenv('CODE_DIR', 'code')
    git_repo_path = os.path.abspath(os.getenv('GIT_REPO_PATH', ''))
    devops_repo_url = os.getenv('DEVOPS_REPO_URL', '')
    scan_interval = int(os.getenv('SCAN_INTERVAL', '3'))
    daily_notification_hour = int(os.getenv('DAILY_NOTIFICATION_HOUR', '10'))
    days_lookback = int(os.getenv('DAYS_LOOKBACK', '3'))
    env_type = os.getenv('ENV_TYPE', '')

    if not db_connection:
        logger.error("Missing required configuration: CONNECTION_STRING")

    if not devops_repo_url:
        logger.error("Missing required configuration: DEVOPS_REPO_URL")

    if not webhook_url:
        logger.error("Missing required configuration: POWER_AUTOMATE_WEBHOOK")
    print( {
        'CONNECTION_STRING': db_connection,
        'OBJECT_PREFIX': object_prefix,
        'DB_FILE': db_file,
        'OUTPUT_DIR': output_dir,
        'POWER_AUTOMATE_WEBHOOK': webhook_url,
        'CODE_DIR': code_dir,
        'GIT_REPO_PATH': git_repo_path,
        'DEVOPS_REPO_URL': devops_repo_url,
        'SCAN_INTERVAL': scan_interval,
        'DAILY_NOTIFICATION_HOUR': daily_notification_hour,
        'DAYS_LOOKBACK': days_lookback,
        'ENV_TYPE': env_type
    })
    return {
        'CONNECTION_STRING': db_connection,
        'OBJECT_PREFIX': object_prefix,
        'DB_FILE': db_file,
        'OUTPUT_DIR': output_dir,
        'POWER_AUTOMATE_WEBHOOK': webhook_url,
        'CODE_DIR': code_dir,
        'GIT_REPO_PATH': git_repo_path,
        'DEVOPS_REPO_URL': devops_repo_url,
        'SCAN_INTERVAL': scan_interval,
        'DAILY_NOTIFICATION_HOUR': daily_notification_hour,
        'DAYS_LOOKBACK': days_lookback,
        'ENV_TYPE': env_type
    }