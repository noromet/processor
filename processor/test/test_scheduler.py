"""
Test cases for the scheduler.py classes and functions.
"""

import unittest
from unittest.mock import patch
from datetime import date

from processor.scheduler import Scheduler


class TestScheduler(unittest.TestCase):
    """
    Test cases for the Scheduler class.
    """

    def setUp(self):
        """Set up test variables."""
        self.tz_names = ["UTC", "Europe/Madrid", "Europe/Lisbon"]

    @patch("processor.database.Database.get_present_timezones")
    def test_get_month_interval_february_leap_year(self, mock_get_tz):
        """Test month interval calculation for February during a leap year."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(date(2024, 2, 15))  # 2024 is a leap year
        start, end = scheduler.get_month_interval()
        self.assertEqual(start.day, 1)
        self.assertEqual(end.day, 29)
        self.assertEqual(end.month, 2)
        self.assertEqual((end - start).days, 28)
        self.assertEqual((end - start).seconds, 86399)

    @patch("processor.database.Database.get_present_timezones")
    def test_get_month_interval_february_non_leap_year(self, mock_get_tz):
        """Test month interval calculation for February during a non-leap year."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(date(2023, 2, 15))  # 2023 is not a leap year
        start, end = scheduler.get_month_interval()
        self.assertEqual(start.day, 1)
        self.assertEqual(end.day, 28)
        self.assertEqual(end.month, 2)
        self.assertEqual((end - start).days, 27)
        self.assertEqual((end - start).seconds, 86399)
