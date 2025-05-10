"""
Test cases for the scheduler.py classes and functions.
"""

import unittest
from unittest.mock import patch
from datetime import date
import zoneinfo

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

    @patch("processor.database.Database.get_present_timezones")
    def test_get_full_day_intervals_normal_day(self, mock_get_tz):
        """Test day interval calculation for a normal day (January 1, 2025)."""
        mock_get_tz.return_value = ["UTC", "Europe/Madrid"]

        # Test for a normal day
        scheduler = Scheduler(date(2025, 1, 1))
        intervals = scheduler.get_full_day_intervals()

        # Check UTC interval
        utc_interval = intervals[next(tz for tz in intervals if tz.key == "UTC")]
        self.assertEqual(utc_interval[0].year, 2025)
        self.assertEqual(utc_interval[0].month, 1)
        self.assertEqual(utc_interval[0].day, 1)
        self.assertEqual(utc_interval[0].hour, 0)
        self.assertEqual(utc_interval[0].minute, 0)
        self.assertEqual(utc_interval[0].second, 0)

        self.assertEqual(utc_interval[1].year, 2025)
        self.assertEqual(utc_interval[1].month, 1)
        self.assertEqual(utc_interval[1].day, 1)
        self.assertEqual(utc_interval[1].hour, 23)
        self.assertEqual(utc_interval[1].minute, 59)
        self.assertEqual(utc_interval[1].second, 59)

        # Duration should be exactly 24 hours - 1 second (in seconds)
        self.assertEqual((utc_interval[1] - utc_interval[0]).total_seconds(), 86399)

        # Print UTC timestamp values
        # print(f"UTC Start Timestamp: {int(utc_interval[0].timestamp())}")
        # print(f"UTC End Timestamp: {int(utc_interval[1].timestamp())}")

        # Check Europe/Madrid interval
        madrid_interval = intervals[
            next(tz for tz in intervals if tz.key == "Europe/Madrid")
        ]
        self.assertEqual(madrid_interval[0].year, 2025)
        self.assertEqual(madrid_interval[0].month, 1)
        self.assertEqual(madrid_interval[0].day, 1)
        self.assertEqual(madrid_interval[0].hour, 0)
        self.assertEqual(madrid_interval[0].minute, 0)
        self.assertEqual(madrid_interval[0].second, 0)

        self.assertEqual(madrid_interval[1].year, 2025)
        self.assertEqual(madrid_interval[1].month, 1)
        self.assertEqual(madrid_interval[1].day, 1)
        self.assertEqual(madrid_interval[1].hour, 23)
        self.assertEqual(madrid_interval[1].minute, 59)
        self.assertEqual(madrid_interval[1].second, 59)

        # Duration should be exactly 24 hours - 1 second (in seconds)
        self.assertEqual(
            (madrid_interval[1] - madrid_interval[0]).total_seconds(), 86399
        )

        # Print Madrid timestamp values
        # print(f"Madrid Start Timestamp: {int(madrid_interval[0].timestamp())}")
        # print(f"Madrid End Timestamp: {int(madrid_interval[1].timestamp())}")

    @patch("processor.database.Database.get_present_timezones")
    def test_get_full_day_intervals_dst_fallback(self, mock_get_tz):
        """
        Test day interval calculation for a day
        with 25 hours (DST fallback, October 27, 2024).
        """

        mock_get_tz.return_value = ["UTC", "Europe/Madrid"]

        # Test for DST fallback day (25 hours in Europe/Madrid)
        scheduler = Scheduler(date(2024, 10, 27))  # DST ends in Europe
        intervals = scheduler.get_full_day_intervals()

        # Check UTC interval (should be normal 24 hours)
        utc_interval = intervals[next(tz for tz in intervals if tz.key == "UTC")]
        self.assertEqual(utc_interval[0].day, 27)
        self.assertEqual(utc_interval[0].hour, 0)
        self.assertEqual(utc_interval[1].day, 27)
        self.assertEqual(utc_interval[1].hour, 23)
        self.assertEqual(
            (utc_interval[1] - utc_interval[0]).total_seconds(), 86399
        )  # 24 hours - 1 second

        # Print UTC timestamp values
        # print(f"DST Fallback - UTC Start Timestamp: {int(utc_interval[0].timestamp())}")
        # print(f"DST Fallback - UTC End Timestamp: {int(utc_interval[1].timestamp())}")

        # Check Europe/Madrid interval (should be 25 hours)
        madrid_interval = intervals[
            next(tz for tz in intervals if tz.key == "Europe/Madrid")
        ]
        self.assertEqual(madrid_interval[0].day, 27)
        self.assertEqual(madrid_interval[0].hour, 0)
        self.assertEqual(madrid_interval[1].day, 27)
        self.assertEqual(madrid_interval[1].hour, 23)

        # Print Madrid timestamp values
        # print(f"DST Fallback - Madrid Start Timestamp: {int(madrid_interval[0].timestamp())}")
        # print(f"DST Fallback - Madrid End Timestamp: {int(madrid_interval[1].timestamp())}")

        # Convert to UTC to verify the actual duration
        # When converting back to UTC, the 25-hour day should be evident
        utc_start = madrid_interval[0].astimezone(zoneinfo.ZoneInfo("UTC"))
        utc_end = madrid_interval[1].astimezone(zoneinfo.ZoneInfo("UTC"))

        # Print UTC-converted timestamp values
        # print(f"DST Fallback - Madrid Start in UTC Timestamp: {int(utc_start.timestamp())}")
        # print(f"DST Fallback - Madrid End in UTC Timestamp: {int(utc_end.timestamp())}")

        # Should be 25 hours - 1 second in total (90000 - 1 = 89999 seconds)
        self.assertEqual((utc_end - utc_start).total_seconds(), 89999)

    @patch("processor.database.Database.get_present_timezones")
    def test_get_full_day_intervals_dst_springforward(self, mock_get_tz):
        """
        Test day interval calculation for a day with
        23 hours (DST spring forward, March 26, 2023).
        """
        mock_get_tz.return_value = ["UTC", "Europe/Madrid"]

        # Test for DST spring forward day (23 hours in Europe/Madrid)
        scheduler = Scheduler(date(2023, 3, 26))  # DST starts in Europe
        intervals = scheduler.get_full_day_intervals()

        # Check UTC interval (should be normal 24 hours)
        utc_interval = intervals[next(tz for tz in intervals if tz.key == "UTC")]
        self.assertEqual(utc_interval[0].day, 26)
        self.assertEqual(utc_interval[0].hour, 0)
        self.assertEqual(utc_interval[1].day, 26)
        self.assertEqual(utc_interval[1].hour, 23)
        self.assertEqual(
            (utc_interval[1] - utc_interval[0]).total_seconds(), 86399
        )  # 24 hours - 1 second

        # Print UTC timestamp values
        # print(f"DST Spring Forward - UTC Start Timestamp: {int(utc_interval[0].timestamp())}")
        # print(f"DST Spring Forward - UTC End Timestamp: {int(utc_interval[1].timestamp())}")

        # Check Europe/Madrid interval (should be 23 hours)
        madrid_interval = intervals[
            next(tz for tz in intervals if tz.key == "Europe/Madrid")
        ]
        self.assertEqual(madrid_interval[0].day, 26)
        self.assertEqual(madrid_interval[0].hour, 0)
        self.assertEqual(madrid_interval[1].day, 26)
        self.assertEqual(madrid_interval[1].hour, 23)

        # Print Madrid timestamp values
        # print(f"DST Spring Forward - Madrid Start
        # Timestamp: {int(madrid_interval[0].timestamp())}")
        # print(f"DST Spring Forward - Madrid End Timestamp: {int(madrid_interval[1].timestamp())}")

        # Convert to UTC to verify the actual duration
        # When converting back to UTC, the 23-hour day should be evident
        utc_start = madrid_interval[0].astimezone(zoneinfo.ZoneInfo("UTC"))
        utc_end = madrid_interval[1].astimezone(zoneinfo.ZoneInfo("UTC"))

        # Print UTC-converted timestamp values
        # print(f"DST Spring Forward - Madrid Start in UTC Timestamp: {int(utc_start.timestamp())}")
        # print(f"DST Spring Forward - Madrid End in UTC Timestamp: {int(utc_end.timestamp())}")

        # Should be 23 hours - 1 second in total (82800 - 1 = 82799 seconds)
        self.assertEqual((utc_end - utc_start).total_seconds(), 82799)
