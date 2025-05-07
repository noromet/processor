"""
Test cases for the processor.py classes and functions.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import date, datetime
import zoneinfo

import pandas as pd

from processor import Processor
from processor.schema import WeatherStation
from processor.builders import BaseBuilder


class TestProcessor(unittest.TestCase):
    """Test cases for the Processor class."""

    def setUp(self):
        """Set up test variables."""

        self.process_date = date(2024, 4, 15)
        self.db_url = "test-db-url"
        self.dry_run = True
        self.mode = "daily"
        self.process_pending = False

        self.tz_names = ["UTC", "Europe/Madrid", "Europe/Lisbon"]

        # Create mock stations for reuse in tests
        self.mock_stations = [
            WeatherStation(
                ws_id="station-1", location="Location 1", local_timezone="UTC"
            ),
            WeatherStation(
                ws_id="station-2", location="Location 2", local_timezone="Europe/Madrid"
            ),
            WeatherStation(
                ws_id="station-3", location="Location 3", local_timezone="Europe/Lisbon"
            ),
        ]

        # Set up common patchers for all tests
        self.get_all_stations_patcher = patch(
            "processor.database.Database.get_all_stations"
        )
        self.get_single_station_patcher = patch(
            "processor.database.Database.get_single_station"
        )

        # Start the patchers
        self.mock_get_all_stations = self.get_all_stations_patcher.start()
        self.mock_get_single_station = self.get_single_station_patcher.start()

        # Set default return values
        self.mock_get_all_stations.return_value = self.mock_stations
        self.mock_get_single_station.return_value = self.mock_stations[
            0
        ]  # Return the first mock station by default

    def tearDown(self):
        """Clean up after each test."""
        # Stop all patchers
        self.get_all_stations_patcher.stop()
        self.get_single_station_patcher.stop()

    def get_processor(self, all_stations=False, station_id=None):
        """Create a Processor instance for testing."""
        return Processor(
            db_url=self.db_url,
            dry_run=self.dry_run,
            process_date=self.process_date,
            mode=self.mode,
            all_stations=all_stations,
            station_id=station_id,
            process_pending=self.process_pending,
        )

    def test_processor_initialization(self):
        """Test Processor class initialization."""
        # Test with a specific station ID
        processor_instance = self.get_processor(station_id="station-1")
        self.assertEqual(processor_instance.db_url, self.db_url)
        self.assertEqual(processor_instance.dry_run, self.dry_run)
        self.assertEqual(processor_instance.date, self.process_date)
        self.assertEqual(processor_instance.mode, self.mode)
        self.assertIsNotNone(processor_instance.run_id)
        self.assertIsNotNone(processor_instance.thread)
        self.assertIsNone(processor_instance.scheduler)
        self.assertEqual(len(processor_instance.stations), 1)
        self.mock_get_single_station.assert_called_once_with("station-1")

        # Reset mock for the next test
        self.mock_get_single_station.reset_mock()

        # Test with all stations
        processor_instance_2 = self.get_processor(all_stations=True)
        self.assertEqual(len(processor_instance_2.stations), 3)
        self.mock_get_all_stations.assert_called_once()

    def test_initialization_with_invalid_parameters(self):
        """
        Test that initialization raises ValueError when both
        all_stations and station_id are specified.
        """
        with self.assertRaises(ValueError):
            Processor(
                db_url=self.db_url,
                dry_run=self.dry_run,
                process_date=self.process_date,
                mode=self.mode,
                all_stations=True,
                station_id="station-1",
                process_pending=self.process_pending,
            )

    def test_get_all_stations(self):
        """Test the get_all_stations method retrieves all active stations."""
        processor = self.get_processor()
        stations = processor.get_all_stations()

        self.mock_get_all_stations.assert_called_once()
        self.assertEqual(stations, self.mock_stations)

        # Test with no active stations
        self.mock_get_all_stations.return_value = []
        with patch("logging.error") as mock_log_error:
            empty_stations = processor.get_all_stations()
            mock_log_error.assert_called_once_with("No active stations found!")
            self.assertEqual(empty_stations, [])

        # Reset mock return value for future tests
        self.mock_get_all_stations.return_value = self.mock_stations

    def test_get_single_station(self):
        """Test the get_single_station method retrieves a station by its ID."""
        processor = self.get_processor()
        stations = processor.get_single_station("station-1")

        self.mock_get_single_station.assert_called_with("station-1")
        self.assertEqual(stations, [self.mock_stations[0]])

        # Test with non-existent station ID
        self.mock_get_single_station.return_value = None
        with patch("logging.error") as mock_log_error:
            empty_stations = processor.get_single_station("non-existent")
            mock_log_error.assert_called_once_with(
                "Station with ID %s not found.", "non-existent"
            )
            self.assertEqual(empty_stations, [])

        # Reset mock return value for future tests
        self.mock_get_single_station.return_value = self.mock_stations[0]

    @patch("processor.database.Database.get_daily_records_for_station_and_interval")
    @patch("processor.scheduler.Scheduler.get_month_interval")
    def test_fill_up_monthly_queue(
        self, mock_get_month_interval, mock_get_daily_records
    ):
        """Test the fill_up_monthly_queue method correctly fills the processing queue."""
        # Create mock month interval
        utc_tz = zoneinfo.ZoneInfo("UTC")
        month_start = datetime(2024, 4, 1, 0, 0, 0, tzinfo=utc_tz)
        month_end = datetime(2024, 4, 30, 23, 59, 59, tzinfo=utc_tz)

        mock_get_month_interval.return_value = (month_start, month_end)

        # Create mock daily records
        mock_daily_records = [
            {"date": date(2024, 4, 1), "max_temperature": 25.0},
            {"date": date(2024, 4, 2), "max_temperature": 27.0},
        ]

        mock_get_daily_records.return_value = mock_daily_records

        # Create processor with mocked stations
        processor = self.get_processor(all_stations=True)
        processor.scheduler = MagicMock()
        processor.scheduler.get_month_interval.return_value = (month_start, month_end)

        # Test fill_up_monthly_queue
        with patch.object(processor.processing_queue, "put") as mock_queue_put:
            processor.fill_up_monthly_queue()

            # Should be called once for each station (3 stations)
            self.assertEqual(mock_queue_put.call_count, 3)

        # Test with no records found
        mock_get_daily_records.return_value = []

        with patch("logging.warning") as mock_log_warning:
            with patch.object(processor.processing_queue, "put") as mock_queue_put:
                processor.fill_up_monthly_queue()

                # Check that warning is logged for each station
                self.assertEqual(mock_log_warning.call_count, 3)
                mock_queue_put.assert_not_called()

    @patch("processor.database.Database.delete_monthly_update_queue_item")
    @patch("processor.database.Database.get_daily_records_for_station_and_interval")
    @patch("processor.database.Database.get_monthly_update_queue_items")
    def test_fill_up_queue_with_pending(
        self, mock_get_queue_items, mock_get_daily_records, mock_delete_queue_item
    ):
        """Test the fill_up_queue_with_pending method processes pending records correctly."""
        # Create mock queue items
        QueueItem = type(
            "QueueItem",
            (),
            {"id": 1, "station_id": "station-1", "year": 2024, "month": 4},
        )
        mock_queue_items = [QueueItem()]
        mock_get_queue_items.return_value = mock_queue_items

        # Create mock daily records
        mock_daily_records = [
            {"date": date(2024, 4, 1), "max_temperature": 25.0},
            {"date": date(2024, 4, 2), "max_temperature": 27.0},
        ]
        mock_get_daily_records.return_value = mock_daily_records

        # Create processor
        processor = self.get_processor()

        # Test fill_up_queue_with_pending
        with patch.object(processor.processing_queue, "put") as mock_queue_put:
            with patch("logging.info") as mock_log_info:
                processor.fill_up_queue_with_pending()

                # Check queue_put is called once
                mock_queue_put.assert_called_once()
                # Check queue item is deleted
                mock_delete_queue_item.assert_called_once_with(1)
                # Check info is logged
                self.assertTrue(mock_log_info.call_count >= 2)

        # Test with empty queue
        mock_get_queue_items.return_value = []

        with patch("logging.info") as mock_log_info:
            with patch.object(processor.processing_queue, "put") as mock_queue_put:
                processor.fill_up_queue_with_pending()

                mock_queue_put.assert_not_called()
                mock_log_info.assert_called_once_with("No pending records to process.")

        # Test with non-existent station
        mock_get_queue_items.return_value = mock_queue_items
        self.mock_get_single_station.return_value = None

        with patch("logging.error") as mock_log_error:
            with patch.object(processor.processing_queue, "put") as mock_queue_put:
                processor.fill_up_queue_with_pending()

                mock_queue_put.assert_not_called()
                mock_log_error.assert_called_once()

        # Test with no daily records
        self.mock_get_single_station.return_value = self.mock_stations[0]
        mock_get_daily_records.return_value = []

        with patch("logging.warning") as mock_log_warning:
            with patch.object(processor.processing_queue, "put") as mock_queue_put:
                processor.fill_up_queue_with_pending()

                mock_queue_put.assert_not_called()
                mock_log_warning.assert_called_once()

    def test_process_queue(self):
        """Test the process_queue method processes all items in the queue."""
        # Create processor
        processor = self.get_processor()

        # Create mock builders
        mock_builder1 = MagicMock(BaseBuilder)
        mock_builder1.station = MagicMock(WeatherStation)
        mock_builder1.station.ws_id = "station-1"
        mock_builder1.station.location = "Location 1"
        mock_builder1.records = pd.DataFrame([1, 2, 3])
        mock_builder1.run.return_value = True

        mock_builder2 = MagicMock(BaseBuilder)
        mock_builder2.station = MagicMock(WeatherStation)
        mock_builder2.station.ws_id = "station-2"
        mock_builder2.station.location = "Location 2"
        mock_builder2.records = pd.DataFrame([4, 5, 6])
        mock_builder2.run.return_value = False

        # Mocking the queue's not_empty method
        processor.processing_queue = MagicMock()
        processor.processing_queue.empty.side_effect = [False, False, True]
        processor.processing_queue.get.side_effect = [mock_builder1, mock_builder2]

        # Test process_queue
        with patch("logging.info") as mock_log_info:
            with patch("logging.error") as mock_log_error:
                processor.process_queue()

                # Check builders were run
                mock_builder1.run.assert_called_once_with(processor.dry_run)
                mock_builder2.run.assert_called_once_with(processor.dry_run)

                # Check logs
                mock_log_info.assert_any_call("Successfully processed %s", "station-1")
                mock_log_error.assert_called_once_with(
                    "Did not process %s", "station-2"
                )

        # Test with non-Processor item in queue
        processor.processing_queue.empty.side_effect = [False, True]
        processor.processing_queue.get.side_effect = ["not a processor"]

        with patch("logging.error") as mock_log_error:
            processor.process_queue()
            mock_log_error.assert_called_once_with(
                "Processor is not of type BaseBuilder."
            )


if __name__ == "__main__":
    unittest.main()
