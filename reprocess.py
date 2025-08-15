"""
Utility to reprocess data for a given year and optionally month.
"""

import argparse
import os
import datetime

MAIN_FILE_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "main.py"))


def get_args():
    """
    Parse and validate command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description="Record reprocessing.")
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to reprocess.",
    )
    parser.add_argument(
        "--month",
        type=int,
        required=False,
        help="Month to reprocess.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["monthly", "daily"],
    )
    parser.add_argument(
        "--station-id",
        type=str,
        required=False,
        default=None,
        help="Station ID to reprocess.",
    )

    args = parser.parse_args()
    return args


def rerun_daily(station_id, year, month):
    """
    Reprocess daily data for a given year and optionally a specific month.

    Args:
        station_id (str, optional): Station ID to reprocess. If None, all stations are reprocessed.
        year (int): Year to reprocess.
        month (int, optional): Month to reprocess. If None, entire year is reprocessed.
    """
    dates = []
    # if month is none, get the range of dates for the whole year. otherwise, for the whole month
    if month is None:
        # Get the range of dates for the whole year
        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        # Get the range of dates for the whole month
        start_date = datetime.date(year, month, 1)
        end_date = (
            datetime.date(year, month + 1, 1)
            if month < 12
            else datetime.date(year + 1, 1, 1)
        ) - datetime.timedelta(days=1)

    # Generate the list of dates
    current_date = start_date
    while current_date <= end_date:
        dates.append((current_date.year, current_date.month, current_date.day))
        current_date += datetime.timedelta(days=1)

    # Print the dates for debugging
    print(f"Dates to reprocess: {dates}")

    # Call the main.py script with the generated dates
    for y, m, d in dates:
        # Construct the command to run main.py with the date arguments
        command = (
            f"python {MAIN_FILE_LOCATION} --mode "
            f"daily --year {y} --month {m} --day {d}"
        )

        if station_id:
            command += f" --id {station_id}"
        else:
            command += " --all"

        print(f"Running command: {command}")
        os.system(command)


def rerun_monthly(station_id, year):
    """
    Reprocess monthly data for a given year.

    Args:
        station_id (str, optional): Station ID to reprocess. If None, all stations are reprocessed.
        year (int): Year to reprocess.
    """
    # Get the range of months for the whole year
    start_month = 1
    end_month = 12

    # Generate the list of months
    months = []
    for month in range(start_month, end_month + 1):
        months.append((year, month))

    # Print the months for debugging
    print(f"Months to reprocess: {months}")

    # Call the main.py script with the generated months
    for y, m in months:
        # Construct the command to run main.py with the date arguments
        command = f"python {MAIN_FILE_LOCATION} --mode monthly --year {y} --month {m}"

        if station_id:
            command += f" --id {station_id}"
        else:
            command += " --all"

        print(f"Running command: {command}")
        os.system(command)


def main():
    """
    Main entry point for the reprocessing script.
    Parses arguments and invokes the appropriate reprocessing function based on mode.
    """
    args = get_args()

    # Check if the main.py file exists
    if not os.path.exists(MAIN_FILE_LOCATION):
        print(f"Error: {MAIN_FILE_LOCATION} does not exist.")
        return

    mode = args.mode

    match mode:
        case "daily":
            rerun_daily(args.station_id, args.year, args.month)
        case "monthly":
            if args.month is not None:
                print("Error: Only year argument is allowed for monthly reprocessing.")
                return
            rerun_monthly(args.station_id, args.year)
        case _:
            print(f"Error: Unknown mode {mode}.")
            return


if __name__ == "__main__":
    main()
