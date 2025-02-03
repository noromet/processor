import os
from dotenv import load_dotenv
import argparse
from datetime import datetime, timedelta
import time
import json
from log import config_logger
import logging

from processors.daily_processor import DailyProcessor
from processors.monthly_processor import MonthlyProcessor

# from processors.yearly_processor import YearlyProcessor

from database import (
    Database,
    get_all_stations,
    get_single_station,
    get_present_timezones,
)
import pytz

# region definitions
load_dotenv(verbose=True)
# endregion


# region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Cook all stations")
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
        "--pretend-its-date",
        type=str,
        help="Pretend it's a specific date in the past. Iso format.",
    )
    return parser.parse_args()


def validate_args(args):
    if args.all and args.id:
        raise ValueError("Cannot specify both --all and --id")

    if not args.all and not args.id:
        raise ValueError("Must specify --all or --id")

    if args.mode == "yearly":
        raise ValueError("Yearly mode not implemented")


# endregion


# region main
def main():
    config_logger()

    db_url = os.getenv("DATABASE_CONNECTION_URL")
    Database.initialize(db_url)

    logging.info("Connected to database.")

    args = get_args()
    validate_args(args)

    global DRY_RUN
    DRY_RUN = args.dry_run
    if args.dry_run:
        logging.info("Dry run enabled.")
    else:
        logging.info("Dry run disabled.")

    single_thread = args.single_thread
    mode = args.mode

    if args.pretend_its_date is None:
        now = datetime.now(pytz.utc)
    else:
        now = datetime.fromisoformat(args.pretend_its_date)
        now = now.replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)

    print("NOW:", now)

    stations = []
    if args.id:
        stations = [
            get_single_station(
                args.id,
            )
        ]
    else:
        stations = get_all_stations()

    if len(stations) == 0:
        logging.error("No active stations found!")
        Database.close_all_connections()
        return

    timezones = [pytz.timezone(tzname) for tzname in get_present_timezones()]

    # calculate all 00:00-23:59 intervals (full days) that have ended in the past 24 hours for each timezone
    past_24_hours = now - timedelta(days=1)

    if mode == "daily":
        full_days_intervals = {}

        for tz in timezones:
            now_tz = now.astimezone(tz)
            past_24_hours_tz = past_24_hours.astimezone(tz)

            # Calculate the start and end of the full day intervals
            start_of_day = tz.localize(
                datetime(
                    past_24_hours_tz.year, past_24_hours_tz.month, past_24_hours_tz.day
                )
            )
            end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)

            if end_of_day < now_tz:
                full_days_intervals[tz] = (start_of_day, end_of_day)
        # full_days_intervals now contains the intervals for each timezone

        if len(full_days_intervals) == 0:
            logging.error("Logic error: no full days found")
            Database.close_all_connections()
            return

        processors = []
        for tz, interval in full_days_intervals.items():
            date_on_tz = start_of_day.astimezone(tz).date()
            logging.info(
                f"Working with timezone: {tz}. Interval: {interval}. Date: {date_on_tz}"
            )
            logging.info(f"Processing mode: {mode}.")

            stations_for_tz = [station for station in stations if station[2] == tz.zone]

            if len(stations_for_tz) == 0:
                logging.error(f"Logic error: no stations in timezone {tz}")
                continue

            station_ids = [station[0] for station in stations_for_tz]

            processors.append(
                DailyProcessor(
                    station_set=station_ids,
                    single_thread=single_thread,
                    interval=interval,
                    timezone=tz,
                    dry_run=DRY_RUN,
                    date=date_on_tz,
                )
            )

    elif mode == "monthly":
        full_months_intervals = {}
        tz = pytz.utc

        if now.day == 1:
            logging.error(
                "Error: running monthly processor on the first day of the month will lead to inconsistencies."
            )
            return

        # we do not care about timezones here
        # get the previous month start and end timestamps

        first_of_current = datetime(now.year, now.month, 1, 0, 0, 0, 0, tz)
        # Go back one day and get first of previous month
        last_of_previous = first_of_current - timedelta(days=1)
        start_of_month = datetime(last_of_previous.year, last_of_previous.month, 1, 0, 0, 0, 0, tz)
        end_of_month = first_of_current - timedelta(seconds=1)

        interval = (start_of_month, end_of_month)

        processors = []
        logging.info(
            f"Processing mode: {mode}. Current timestamp in timezone: {datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S %z')}"
        )
        logging.info(
            f"Working with hardcoded timezone: {tz}. Interval: {interval}. Date: {start_of_month.date()}"
        )

        station_ids = [station[0] for station in stations]

        processors.append(
            MonthlyProcessor(
                station_set=station_ids,
                single_thread=single_thread,
                interval=interval,
                dry_run=DRY_RUN,
            )
        )

    else:
        raise ValueError("Invalid mode")

    for i, processor in enumerate(processors):
        logging.info(f"Running {processor.__class__.__name__} for {len(processor.station_set)} stations")
        processor.run()

        if i < len(processors) - 1:
            logging.info(f"Finished {processor.__class__.__name__}. Sleeping for 2 seconds.")
            time.sleep(2)
        else:
            logging.info(f"Finished {processor.__class__.__name__}")

    logging.info("All processors finished.")
    Database.close_all_connections()

    logging.info("Exiting.")


if __name__ == "__main__":
    main()

# endregion
