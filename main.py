"""
Weather Record processing module.
Runs daily or monthly processing of weather records.
"""

import os
import argparse
from datetime import date
import logging

from dotenv import load_dotenv

from processor import Processor
from processor.database import database_connection

load_dotenv(verbose=True, dotenv_path=".env")


def get_args():
    """
    Parse command line arguments for the weather station data processor.
        :return: Parsed arguments.
        :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Process all stations")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument(
        "--process-pending", action="store_true", help="Process pending records"
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "monthly"],
        required=True,
        help="Processing mode",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year component of the date.",
        required=True,
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Month component of the date.",
    )
    parser.add_argument(
        "--day",
        type=int,
        help="Day component of the date.",
    )

    args = parser.parse_args()

    if args.all and args.id:
        raise ValueError("Cannot specify both --all and --id")

    if not args.all and not args.id:
        raise ValueError("Must specify --all or --id")

    if args.mode == "yearly":
        raise ValueError("Yearly mode not implemented")

    if args.mode == "daily" and args.day is None:
        raise ValueError("Must specify a --day for daily mode")

    if args.mode == "monthly" and args.month is None:
        raise ValueError("Must specify a --month for monthly mode")

    if args.mode == "monthly" and args.day is not None:
        raise ValueError("Cannot specify a --day for monthly mode")

    # Build a date object for the processing date
    if args.mode == "daily":
        args.date = date(args.year, args.month, args.day)
    elif args.mode == "monthly":
        args.date = date(args.year, args.month, 1)
    else:
        args.date = date(args.year, 1, 1)

    args.db_url = os.getenv("DATABASE_CONNECTION_URL", "")

    return args


def main():
    """Main function to run the weather record processing."""

    args = get_args()

    with database_connection(args.db_url):
        logging.info("Connected to database.")

        main_instance = Processor(
            dry_run=args.dry_run,
            process_date=args.date,
            mode=args.mode,
            all_stations=args.all,
            station_id=args.id,
            process_pending=args.process_pending,
        )

        main_instance.run()


if __name__ == "__main__":
    main()
