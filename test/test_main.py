"""
Test cases for the main.py classes and functions.
"""

import logging
import unittest
from unittest.mock import patch
from datetime import date
import zoneinfo

from main import Scheduler

# silence logs
logging.disable(logging.CRITICAL)


class TestScheduler(unittest.TestCase):
    """
    Test cases for the Scheduler class.
    """

    def setUp(self):
        """Set up test variables for Scheduler tests."""
        self.test_date = date(2024, 4, 15)
        self.tz_names = ["UTC", "Europe/Madrid"]

    @patch("main.Database.get_present_timezones")
    def test_scheduler_initialization(self, mock_get_tz):
        """Test Scheduler initialization with mocked timezones."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(self.test_date)
        self.assertEqual(scheduler.process_date, self.test_date)
        self.assertEqual([tz.key for tz in scheduler.timezones], self.tz_names)

    @patch("main.Database.get_present_timezones")
    def test_get_full_day_intervals(self, mock_get_tz):
        """Test full day intervals for all timezones."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(self.test_date)
        intervals = scheduler.get_full_day_intervals()
        self.assertEqual({tz.key for tz in intervals}, set(self.tz_names))
        for tz, (start, end) in intervals.items():
            self.assertEqual(start.tzinfo, tz)
            self.assertEqual(start.hour, 0)
            self.assertEqual(start.minute, 0)
            self.assertEqual(start.second, 0)
            self.assertEqual(start.year, self.test_date.year)
            self.assertEqual(start.month, self.test_date.month)
            self.assertEqual(start.day, self.test_date.day)
            self.assertEqual((end - start).days, 0)
            self.assertEqual((end - start).seconds, 24 * 3600 - 1)

    @patch("main.Database.get_present_timezones")
    def test_get_month_interval(self, mock_get_tz):
        """Test month interval calculation for a given date."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(self.test_date)
        start, end = scheduler.get_month_interval()
        self.assertEqual(start.tzinfo, zoneinfo.ZoneInfo("UTC"))
        self.assertEqual(start.day, 1)
        self.assertEqual(start.month, self.test_date.month)
        self.assertEqual(start.year, self.test_date.year)

        self.assertEqual(end.month, self.test_date.month)
        self.assertEqual(end.year, self.test_date.year)
        self.assertEqual(end.hour, 23)
        self.assertEqual(end.minute, 59)
        self.assertEqual(end.second, 59)

        self.assertEqual((end - start).days, 29)
        self.assertEqual((end - start).seconds, 86399)

    @patch("main.Database.get_present_timezones")
    def test_get_month_interval_december(self, mock_get_tz):
        """Test month interval calculation for December."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(date(2023, 12, 15))
        start, end = scheduler.get_month_interval()
        self.assertEqual(start.month, 12)
        self.assertEqual(end.month, 12)
        self.assertEqual(end.day, 31)
