import os
from dotenv import load_dotenv
import argparse
import datetime
import time

from log import config_logger
import logging

from processors.daily_processor import DailyProcessor
from processors.monthly_processor import MonthlyProcessor
# from processors.yearly_processor import YearlyProcessor

from database import Database, get_all_stations, get_single_station


# region definitions
load_dotenv(verbose=True)
# endregion

#region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Cook all stations")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("--today", action="store_true", help="Shorthand to process today's daily data")
    parser.add_argument("--yesterday", action="store_true", help="Shorthand to process yesterday's daily data")
    parser.add_argument("--mode", choices=["daily", "monthly", "yearly", "auto"], default="daily", help="Processing mode")
    parser.add_argument("--date", type=datetime.date.fromisoformat, help="Date to process, in ISO format")
    parser.add_argument("--single-thread", action="store_true", default=False, help="Force single threaded execution")
    return parser.parse_args()

def validate_args(args):
    if args.all and args.id:
        raise ValueError("Cannot specify both --all and --id")
    
    if not args.all and not args.id:
        raise ValueError("Must specify --all or --id")
    
    #date needs to be validated: yesterday or before
    if args.date:
        if args.date > datetime.date.today():
            raise ValueError("Date must be yesterday's or earlier")
        if not args.mode:
            raise ValueError("Must specify --mode with --date")
        
    #today and yesterday are mutually exclusive
    if args.today and args.yesterday:
        raise ValueError("Cannot specify both --today and --yesterday")
    
    #either of the previous overrides date
    if args.today:
        args.date = datetime.date.today()
    elif args.yesterday:
        args.date = datetime.date.today() - datetime.timedelta(days=1)

    if not args.date and not args.today and not args.yesterday and args.mode != "auto":
        raise ValueError("Must specify --date, --today or --yesterday")
    
    if args.mode == "yearly":
        raise ValueError("Yearly mode not implemented")
#endregion

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


    station_ids = []
    if args.id:
        station_ids = [get_single_station(args.id, )[0]]
    else:
        station_ids = [station[0] for station in get_all_stations()]

    if len(station_ids) == 0:
        logging.error("No active stations found!")
        Database.close_all_connections()
        return
    
    today = datetime.datetime.now().date()
    yesterday = today - datetime.timedelta(days=1)

    logging.info(f"Processing mode: {mode}. Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    processors = []
    match mode:
        case "daily":
            processors.append(DailyProcessor(station_set = station_ids, single_thread = single_thread, date = args.date, dry_run = DRY_RUN))
        case "monthly":
            processors.append(MonthlyProcessor(station_set = station_ids, single_thread = single_thread, date = args.date, dry_run = DRY_RUN))
            pass
        case "yearly":
            # processor.append(YearlyProcessor(station_set = station_ids, single_thread = single_thread, date = args.date))
            pass
        case "auto":
            # daily runs every day for yesterday's data
            # monthly runs on the first day of the month, for last month's data
            # yearly runs on the first day of the year, for last year's data, but not yet implemented

            processors.append(DailyProcessor(station_set = station_ids, single_thread = single_thread, date = yesterday, dry_run = DRY_RUN))

            if today.day == 6:
                processors.append(MonthlyProcessor(station_set = station_ids, single_thread = single_thread, date = yesterday, dry_run = DRY_RUN))

            # if now.month == 1 and now.day == 1:
            #     processors.append(YearlyProcessor(station_set = station_ids, single_thread = single_thread, date = yesterday))

        case _:
            raise ValueError("Invalid mode")

    for i, processor in enumerate(processors):
        logging.info(f"Running {processor.__class__.__name__} for {len(processor.station_set)} stations")
        processor.run()

        if i < len(processors) - 1:
            logging.info(f"Finished {processor.__class__.__name__}. Sleeping for 5 seconds.")
            time.sleep(5)
        else:
            logging.info(f"Finished {processor.__class__.__name__}")

    logging.info("All processors finished.")
    Database.close_all_connections()

    logging.info("Exiting.")

    
if __name__ == "__main__":
    main()

# endregion