"""
Weather Record processing module.
Runs daily or monthly processing of weather records.
"""

import os
import argparse
import logging
from datetime import datetime, timedelta, date
import zoneinfo
import queue
import uuid

import pandas as pd
from dotenv import load_dotenv

from log import config_logger
from processors import DailyProcessor, MonthlyProcessor
from database import Database, database_connection

load_dotenv(verbose=True)


# region argument processing
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
        "--mode",
        choices=["daily", "monthly", "yearly"],
        default="daily",
        help="Processing mode",
    )
    parser.add_argument(
        "--single-thread",
        action="store_true",
        default=False,
        help="Force single threaded execution",
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

    if args.single_thread:
        args.max_threads = 1
    else:
        args.max_threads = os.cpu_count() or 1

    return args


# endregion


# region Scheduler
class Scheduler:
    """Scheduler for weather record processing intervals."""

    def __init__(self, process_date: date):
        if not isinstance(process_date, date):
            raise ValueError("process_date must be a datetime.date instance")
        self.process_date = process_date
        self.timezones = [
            zoneinfo.ZoneInfo(tzname) for tzname in Database.get_present_timezones()
        ]

        logging.info("Scheduler initialized.")
        logging.info("Processing date: %s", self.process_date.isoformat())
        logging.info("Available timezones: %s", [tz.key for tz in self.timezones])

    def get_full_day_intervals(self):
        """Get start and end datetimes for the full day in each timezone."""
        full_day_intervals = {}
        for tz in self.timezones:
            # Build a datetime for the start of the day in the given timezone
            start_of_day = datetime(
                self.process_date.year,
                self.process_date.month,
                self.process_date.day,
                0,
                0,
                0,
                0,
                tz,
            )
            end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
            full_day_intervals[tz] = (start_of_day, end_of_day)
        return full_day_intervals

    def get_month_interval(self):
        """Get start and end datetimes for the month interval in UTC timezone."""
        tz = zoneinfo.ZoneInfo("UTC")
        start_of_month = datetime(
            self.process_date.year, self.process_date.month, 1, 0, 0, 0, 0, tz
        )
        if self.process_date.month == 12:
            next_month = datetime(self.process_date.year + 1, 1, 1, 0, 0, 0, 0, tz)
        else:
            next_month = datetime(
                self.process_date.year, self.process_date.month + 1, 1, 0, 0, 0, 0, tz
            )
        end_of_month = next_month - timedelta(seconds=1)
        return (start_of_month, end_of_month)


# endregion


# region utils
def get_stations(station_id: str | None = None, all_stations: bool = False):
    """Retrieve stations by ID or all stations if requested."""
    if station_id is not None:
        station = Database.get_single_station(station_id)

        if station is None:
            logging.error("Station with ID %s not found.", station_id)
            return []
        return [station]

    if all_stations:
        stations = Database.get_all_stations()
        if len(stations) == 0:
            logging.error("No active stations found!")
            return []
        return stations

    raise ValueError("Invalid station ID or all_stations flag.")


# endregion


# region main
def main():
    """Main entry point for weather record processing."""
    config_logger()
    args = get_args()
    run_id = str(uuid.uuid4())

    with database_connection(args.db_url):
        logging.info("Connected to database.")

        if args.dry_run:
            logging.info("Dry run enabled.")
        else:
            logging.warning("Dry run disabled.")

        scheduler = Scheduler(args.date)
        stations = get_stations(args.id, args.all)
        processing_queue = queue.Queue()

        match args.mode:
            case "daily":
                full_days_intervals = scheduler.get_full_day_intervals()

                for tz, interval in full_days_intervals.items():
                    date_on_tz = interval[0].astimezone(tz).date()
                    stations_for_tz = [
                        station for station in stations if station.local_timezone == tz
                    ]  # to avoid extra DB query

                    for station in stations_for_tz:
                        records = Database.get_weather_records_for_station_and_interval(
                            station_id=str(station.ws_id),
                            date_from=interval[0],
                            date_to=interval[1],
                        )

                        processing_queue.put(
                            DailyProcessor(
                                station=station,
                                records=pd.DataFrame(records),
                                date=date_on_tz,
                                run_id=run_id,
                            )
                        )

            case "monthly":
                tz = zoneinfo.ZoneInfo("UTC")

                month_interval = scheduler.get_month_interval()

                for station in stations:
                    records = Database.get_daily_records_for_station_and_interval(
                        station_id=str(station.ws_id),
                        start_date=month_interval[0],
                        end_date=month_interval[1],
                    )

                    processing_queue.put(
                        MonthlyProcessor(
                            station=station,
                            records=pd.DataFrame(records),
                            interval=month_interval,
                            run_id=run_id,
                        )
                    )

        logging.info("Starting processing. Run ID: %s", run_id)

        while not processing_queue.empty():
            processor = processing_queue.get()
            logging.info(
                "Processing %s (%d records)",
                processor.station.location,
                len(processor.records),
            )

            result = processor.run()

            if result:
                logging.info("Successfully processed %s", processor.station.ws_id)

        logging.info("Processor done.")


if __name__ == "__main__":
    main()

# endregion
