"""
Test cases for the processor.py classes and functions.
"""

import unittest
from unittest.mock import patch
from datetime import date

from processor import Processor


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

    @patch("processor.database.Database.get_single_station")
    @patch("processor.database.Database.get_all_stations")
    def test_processor_initialization(self, get_all_stations, get_single_station):
        """Test Processor class initialization."""

        get_single_station.return_value = [1]
        get_all_stations.return_value = [1, 2, 3]

        processor_instance = self.get_processor(station_id=1)
        self.assertEqual(processor_instance.db_url, self.db_url)
        self.assertEqual(processor_instance.dry_run, self.dry_run)
        self.assertEqual(processor_instance.date, self.process_date)
        self.assertEqual(processor_instance.mode, self.mode)
        self.assertIsNotNone(processor_instance.run_id)
        self.assertIsNotNone(processor_instance.thread)
        self.assertIsNone(processor_instance.scheduler)
        self.assertTrue(len(processor_instance.stations) == 1)

        processor_instance_2 = self.get_processor(all_stations=True)
        self.assertTrue(len(processor_instance_2.stations) == 3)
