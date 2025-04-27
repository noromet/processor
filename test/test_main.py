"""
Test cases for the main.py classes and functions.
"""

import logging
import unittest
from unittest.mock import patch
from datetime import date, datetime
import zoneinfo

from main import Scheduler, Main

# silence logs
logging.disable(logging.CRITICAL)


class TestScheduler(unittest.TestCase):
    """
    Test cases for the Scheduler class.
    """

    def setUp(self):
        """Set up test variables."""
        self.tz_names = ["UTC", "Europe/Madrid", "Europe/Lisbon"]

    @patch("main.Database.get_present_timezones")
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

    @patch("main.Database.get_present_timezones")
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


class TestMain(unittest.TestCase):
    """Test cases for the Main class."""

    def setUp(self):
        """Set up test variables."""
        self.test_date = date(2024, 4, 15)
        self.station_id = "test-station-id"
        self.tz_names = ["UTC", "Europe/Madrid", "Europe/Lisbon"]

    @patch("main.get_args")
    def test_main_initialization(self, mock_get_args):
        """Test Main class initialization."""
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.dry_run = True
        mock_args.date = self.test_date
        mock_args.mode = "daily"
        mock_args.all = False
        mock_args.id = self.station_id
        mock_get_args.return_value = mock_args

        with patch("main.config_logger"):
            main_instance = Main()

        self.assertEqual(main_instance.db_url, "test-db-url")
        self.assertTrue(main_instance.dry_run)
        self.assertEqual(main_instance.date, self.test_date)
        self.assertEqual(main_instance.mode, "daily")
        self.assertFalse(main_instance.all_stations)
        self.assertEqual(main_instance.station_id, self.station_id)
        self.assertIsNotNone(main_instance.run_id)
        self.assertIsNotNone(main_instance.thread)
        self.assertIsNone(main_instance.scheduler)

    @patch("main.Database.get_monthly_update_queue_items")
    @patch("database.Database.get_present_timezones")
    @patch("main.get_args")
    @patch("main.database_connection")
    @patch("main.Main.get_stations")
    def test_run_daily_mode(
        self,
        mock_get_stations,
        mock_db_conn,
        mock_get_args,
        mock_get_tz,
        mock_get_queue,
    ):
        """Test Main.run method in daily mode."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.dry_run = True
        mock_args.date = self.test_date
        mock_args.mode = "daily"
        mock_args.all_stations = False
        mock_args.id = self.station_id
        mock_get_args.return_value = mock_args

        mock_db_conn.return_value.__enter__.return_value = None
        mock_get_stations.return_value = []
        mock_get_tz.return_value = self.tz_names
        mock_get_queue.return_value = []

        # Execute
        with patch("main.config_logger"), patch("main.Database.save_processor_thread"):
            main_instance = Main()
            main_instance.run()

        # Assert
        self.assertEqual(main_instance.date, self.test_date)
        self.assertTrue(mock_db_conn.called)
        self.assertTrue(mock_get_stations.called)

    @patch("main.Database.get_monthly_update_queue_items")
    @patch("main.get_args")
    @patch("main.database_connection")
    @patch("main.Main.get_stations")
    def test_run_monthly_mode(
        self, mock_get_stations, mock_db_conn, mock_get_args, mock_get_queue
    ):
        """Test Main.run method in monthly mode."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.dry_run = True
        mock_args.date = date(2024, 4, 1)  # First day of month for monthly mode
        mock_args.mode = "monthly"
        mock_args.all_stations = False
        mock_args.id = self.station_id
        mock_get_args.return_value = mock_args

        mock_db_conn.return_value.__enter__.return_value = None
        mock_get_stations.return_value = []
        mock_get_queue.return_value = []

        # Execute
        with patch("main.config_logger"), patch("main.Scheduler"), patch(
            "main.Database.save_processor_thread"
        ):
            main_instance = Main()
            main_instance.run()

        # Assert
        self.assertTrue(mock_db_conn.called)
        self.assertTrue(mock_get_stations.called)

    @patch("main.get_args")
    def test_get_stations_with_id(self, mock_get_args):
        """Test get_stations method with a specific ID."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.all_stations = False
        mock_args.id = self.station_id
        mock_get_args.return_value = mock_args

        mock_station = unittest.mock.MagicMock()
        mock_station.ws_id = self.station_id

        # Execute
        with patch("main.config_logger"), patch(
            "main.Database.get_single_station", return_value=mock_station
        ):
            main_instance = Main()
            stations = main_instance.get_stations()

        # Assert
        self.assertEqual(len(stations), 1)
        self.assertEqual(stations[0].ws_id, self.station_id)

    @patch("main.get_args")
    def test_get_stations_with_all_flag(self, mock_get_args):
        """Test get_stations method with all stations flag."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.all_stations = True
        mock_args.id = None
        mock_get_args.return_value = mock_args

        mock_stations = [unittest.mock.MagicMock(), unittest.mock.MagicMock()]
        mock_stations[0].ws_id = "station-1"
        mock_stations[1].ws_id = "station-2"

        # Execute
        with patch("main.config_logger"), patch(
            "main.Database.get_all_stations", return_value=mock_stations
        ):
            main_instance = Main()
            stations = main_instance.get_stations()

        # Assert
        self.assertEqual(len(stations), 2)
        self.assertEqual(stations[0].ws_id, "station-1")
        self.assertEqual(stations[1].ws_id, "station-2")

    @patch("main.get_args")
    def test_fill_up_daily_queue(self, mock_get_args):
        """Test fill_up_daily_queue method."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.date = self.test_date
        mock_args.mode = "daily"
        mock_args.all_stations = True
        mock_args.id = None
        mock_get_args.return_value = mock_args

        # Create mock stations and timezone
        mock_tz = zoneinfo.ZoneInfo("UTC")
        mock_station = unittest.mock.MagicMock()
        mock_station.ws_id = "test-station"
        mock_station.local_timezone = mock_tz
        mock_station.location = "Test Location"

        # Mock intervals and records
        mock_intervals = {
            mock_tz: (
                datetime(2024, 4, 15, tzinfo=mock_tz),
                datetime(2024, 4, 15, 23, 59, 59, tzinfo=mock_tz),
            )
        }
        mock_records = [
            {"date": datetime(2024, 4, 15, 12, 0, tzinfo=mock_tz), "value": 10}
        ]

        # Execute
        with patch("main.config_logger"), patch(
            "main.Main.get_stations", return_value=[mock_station]
        ), patch(
            "main.Database.get_weather_records_for_station_and_interval",
            return_value=mock_records,
        ), patch(
            "pandas.DataFrame"
        ), patch(
            "main.DailyProcessor"
        ) as mock_daily_processor:
            main_instance = Main()
            main_instance.scheduler = unittest.mock.MagicMock()
            main_instance.scheduler.get_full_day_intervals.return_value = mock_intervals
            main_instance.fill_up_daily_queue()

        # Assert
        self.assertTrue(mock_daily_processor.called)
        self.assertEqual(main_instance.processing_queue.qsize(), 1)

    @patch("main.get_args")
    def test_fill_up_monthly_queue(self, mock_get_args):
        """Test fill_up_monthly_queue method."""
        # Setup
        mock_args = unittest.mock.MagicMock()
        mock_args.db_url = "test-db-url"
        mock_args.date = date(2024, 4, 1)
        mock_args.mode = "monthly"
        mock_args.all_stations = True
        mock_args.id = None
        mock_get_args.return_value = mock_args

        # Create mock stations and timezone
        mock_tz = zoneinfo.ZoneInfo("UTC")
        mock_station = unittest.mock.MagicMock()
        mock_station.ws_id = "test-station"
        mock_station.location = "Test Location"

        # Mock intervals and daily records
        mock_interval = (
            datetime(2024, 4, 1, tzinfo=mock_tz),
            datetime(2024, 4, 30, 23, 59, 59, tzinfo=mock_tz),
        )
        mock_records = [{"date": date(2024, 4, 15), "avg_temp": 20}]

        # Execute
        with patch("main.config_logger"), patch(
            "main.Main.get_stations", return_value=[mock_station]
        ), patch(
            "main.Database.get_daily_records_for_station_and_interval",
            return_value=mock_records,
        ), patch(
            "pandas.DataFrame"
        ), patch(
            "main.MonthlyProcessor"
        ) as mock_monthly_processor:
            main_instance = Main()
            main_instance.scheduler = unittest.mock.MagicMock()
            main_instance.scheduler.get_month_interval.return_value = mock_interval
            main_instance.fill_up_monthly_queue()

        # Assert
        self.assertTrue(mock_monthly_processor.called)
        self.assertEqual(main_instance.processing_queue.qsize(), 1)

    @patch("main.Database.get_present_timezones")
    def test_get_month_interval_december(self, mock_get_tz):
        """Test month interval calculation for December."""
        mock_get_tz.return_value = self.tz_names

        scheduler = Scheduler(date(2023, 12, 15))
        start, end = scheduler.get_month_interval()
        self.assertEqual(start.month, 12)
        self.assertEqual(end.month, 12)
        self.assertEqual(end.day, 31)
