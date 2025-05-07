#!/usr/bin/env python3
"""
Docker entrypoint script for the Weather Record processor.
Translates convenience arguments into the format expected by main.py.
"""

import sys
import subprocess
import argparse
from datetime import datetime, timedelta
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("docker_entrypoint")


def parse_args():
    """
    Parse command line arguments and convert them to main.py arguments.

    Returns:
        list: Arguments to pass to main.py
    """
    parser = argparse.ArgumentParser(
        description="Docker entrypoint for Weather Station Data Processor"
    )

    # Add convenience arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--yesterday", action="store_true", help="Process daily data for yesterday"
    )
    group.add_argument(
        "--today", action="store_true", help="Process daily data for today"
    )
    group.add_argument(
        "--last-month",
        action="store_true",
        help="Process monthly data for the previous month",
    )
    group.add_argument(
        "--this-month",
        action="store_true",
        help="Process monthly data for the current month",
    )
    group.add_argument(
        "--date", metavar="YYYY-MM-DD", help="Process daily data for specific date"
    )
    group.add_argument(
        "--month", metavar="YYYY-MM", help="Process monthly data for specific month"
    )
    group.add_argument(
        "--passthrough",
        action="store_true",
        help="Pass all remaining arguments directly to main.py",
    )

    # Add passthrough arguments
    parser.add_argument(
        "passthrough_args",
        nargs=argparse.REMAINDER,
        help="Arguments to pass directly to main.py when using --passthrough",
    )

    # Process options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without modifying data",
    )

    args = parser.parse_args()

    # Build the base arguments for main.py
    base_args = ["python", "main.py", "--all"]
    if args.dry_run:
        base_args.append("--dry-run")

    today = datetime.now()

    # Handle each convenience argument
    if args.yesterday:
        yesterday = today - timedelta(days=1)
        return base_args + [
            "--mode",
            "daily",
            "--year",
            str(yesterday.year),
            "--month",
            str(yesterday.month),
            "--day",
            str(yesterday.day),
        ]

    if args.today:
        return base_args + [
            "--mode",
            "daily",
            "--year",
            str(today.year),
            "--month",
            str(today.month),
            "--day",
            str(today.day),
        ]

    if args.last_month:
        # Calculate last month
        last_month = today.replace(day=1) - timedelta(days=1)
        return base_args + [
            "--mode",
            "monthly",
            "--year",
            str(last_month.year),
            "--month",
            str(last_month.month),
        ]

    if args.this_month:
        return base_args + [
            "--mode",
            "monthly",
            "--year",
            str(today.year),
            "--month",
            str(today.month),
        ]

    if args.date:
        try:
            # Parse YYYY-MM-DD format
            specific_date = datetime.strptime(args.date, "%Y-%m-%d")
            return base_args + [
                "--mode",
                "daily",
                "--year",
                str(specific_date.year),
                "--month",
                str(specific_date.month),
                "--day",
                str(specific_date.day),
            ]
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    if args.month:
        try:
            # Parse YYYY-MM format
            specific_month = datetime.strptime(args.month, "%Y-%m")
            return base_args + [
                "--mode",
                "monthly",
                "--year",
                str(specific_month.year),
                "--month",
                str(specific_month.month),
            ]
        except ValueError:
            logger.error("Invalid month format. Use YYYY-MM")
            sys.exit(1)

    # If passthrough is set, pass all remaining arguments directly to main.py
    if args.passthrough:
        return ["python", "main.py"] + args.passthrough_args

    return base_args


def main():
    """
    Main entrypoint function that converts arguments and calls the main processor.
    """
    cmd_args = parse_args()

    logger.info("Starting processor with args: %s", " ".join(cmd_args))

    # Execute the main.py script with the processed arguments
    try:
        result = subprocess.run(cmd_args, check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        logger.error("Process failed with exit code %d", e.returncode)
        sys.exit(e.returncode)
    except Exception as e:
        logger.exception("Failed to run processor: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
