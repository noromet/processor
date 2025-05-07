"""
Module for scheduling weather record processing intervals.
"""

import logging
from datetime import date, datetime, timedelta
import zoneinfo

from processor.database import Database


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
