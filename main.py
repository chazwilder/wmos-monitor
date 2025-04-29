"""
Main entry point for the WMOS Database Change Tracker
"""

import time
import datetime
import argparse
import sys

from config import load_config
from tracker.change_tracker import ChangeTracker
from utils.logger import logger


def calculate_next_runtime(interval_hours=3):
    """
    Calculate the next runtime based on interval
    
    Args:
        interval_hours (int): Interval in hours
        
    Returns:
        datetime.datetime: Next runtime
    """
    now = datetime.datetime.now()

    # Get the current hour and calculate how many intervals have passed today
    current_hour = now.hour
    intervals_passed = current_hour // interval_hours

    # Calculate the next interval hour
    next_interval_hour = (intervals_passed + 1) * interval_hours

    # If the next interval would be tomorrow, adjust to the first interval of the day (0)
    if next_interval_hour >= 24:
        next_interval_hour = 0
        # Create tomorrow's date at the first interval
        next_time = now.replace(hour=next_interval_hour, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
    else:
        # Create today's date at the next interval
        next_time = now.replace(hour=next_interval_hour, minute=0, second=0, microsecond=0)

    # If the calculated time is in the past, move to the next interval
    if next_time <= now:
        next_time = next_time + datetime.timedelta(hours=interval_hours)

    return next_time


def run_service(config):
    """
    Run the service in continuous mode with scheduled tracking
    
    Args:
        config: Configuration object
    """
    scan_interval = config.get('SCAN_INTERVAL', 3)

    logger.info(f"Starting WMOS Database Change Tracker service (interval: {scan_interval} hours)")
    logger.info(f"Environment: {config.get('ENV_TYPE', 'DEV')}")

    # Initialize counter to ensure first run happens immediately
    counter = 0
    next_runtime = None

    try:
        while True:
            now = datetime.datetime.now()

            # Calculate next runtime only if we don't have one or we've reached/passed it
            if next_runtime is None or now >= next_runtime:
                logger.info(f"Executing scheduled tracking at {now}")

                tracker = ChangeTracker(config)
                tracker.run_tracking_cycle()

                counter += 1

                # Calculate the next runtime after execution
                next_runtime = calculate_next_runtime(scan_interval)
                logger.info(f"Next execution scheduled at {next_runtime}")

            # Calculate sleep time until next run (with buffer to avoid timing issues)
            sleep_seconds = max(0, (next_runtime - datetime.datetime.now()).total_seconds() - 0.1)

            logger.info(f"Next execution scheduled at {next_runtime} (sleeping for {sleep_seconds/60:.1f} minutes)")

            # Sleep until next runtime, but check every 5 minutes to handle system time changes
            sleep_interval = min(300, sleep_seconds)  # 5 minutes = 300 seconds
            if sleep_interval > 0:
                time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Service error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

    logger.info("Service stopped")


def run_single_scan(config):
    """
    Run a single tracking cycle
    
    Args:
        config: Configuration object
    """
    logger.info(f"Starting WMOS Database Change Tracker for single scan")
    logger.info(f"Environment: {config.get('ENV_TYPE', 'DEV')}")

    tracker = ChangeTracker(config)
    results = tracker.run_tracking_cycle()

    if results:
        logger.info(f"Scan completed successfully. Found {results['changed_objects']} changes in {results['total_objects']} objects.")
    else:
        logger.error("Scan failed")
        return 1

    return 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='WMOS Database Change Tracker')
    parser.add_argument('--env', type=str, choices=['dev', 'uat', 'prod'],
                        help='Environment (dev, uat, prod)')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--single-run', action='store_true',
                        help='Run a single tracking cycle and exit')
    parser.add_argument('--send-summary', action='store_true',
                        help='Send daily summary regardless of time')

    args = parser.parse_args()

    # If environment is specified, set ENV_TYPE environment variable
    if args.env:
        import os
        os.environ['ENV_TYPE'] = args.env.upper()

    # Load configuration
    config_file = args.config if args.config else None
    config = load_config(config_file)

    print("config loaded")

    # Check if send-summary is specified
    if args.send_summary:
        logger.info("Sending daily summary...")
        tracker = ChangeTracker(config)
        result = tracker.send_daily_summary()
        if result:
            logger.info("Daily summary sent successfully")
            return 0
        else:
            logger.error("Failed to send daily summary")
            return 1

    # Run in single-run mode or service mode
    if args.single_run:
        return run_single_scan(config)
    else:
        run_service(config)
        return 0


if __name__ == "__main__":
    sys.exit(main())