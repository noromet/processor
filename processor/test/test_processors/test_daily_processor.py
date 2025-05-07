"""
Test cases for the daily_processor.py classes and functions.
"""

import unittest
import datetime

import pandas as pd

from processor.schema import WeatherStation, DailyRecord
from processor.processors.daily_processor import DailyProcessor


class TestDailyProcessor(unittest.TestCase):
    """
    Test suite for the DailyProcessor class that processes daily weather data.
    """

    def setUp(self):
        """
        Set up test environment before each test method.
        Creates a mock weather station, sample records, and processor instance.
        """
        # Create a mock weather station
        self.station = WeatherStation(
            ws_id="test-station",
            location="Test Station",
            local_timezone="Europe/Madrid",
        )

        # Create sample records for testing
        self.date = datetime.date(2024, 4, 15)
        self.records = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "timestamp": pd.to_datetime(
                    ["2024-04-15 08:00", "2024-04-15 12:00", "2024-04-15 16:00"]
                ),
                "temperature": [10.0, 15.0, 12.0],
                "max_temperature": [11.0, 16.0, 13.0],
                "min_temperature": [9.0, 14.0, 11.0],
                "pressure": [1010.0, 1012.0, 1011.0],
                "wind_speed": [5.0, 7.0, 6.0],
                "max_wind_speed": [6.0, 8.0, 7.0],
                "wind_gust": [10.0, 12.0, 11.0],
                "max_wind_gust": [11.0, 13.0, 12.0],
                "wind_direction": [90.0, 180.0, 270.0],
                "cumulative_rain": [0.0, 2.0, 5.0],
                "humidity": [70.0, 60.0, 65.0],
                "flagged": [False, False, False],
            }
        )

        # Create processor
        self.processor = DailyProcessor(
            station=self.station,
            records=self.records,
            date=self.date,
            run_id="test-run-id",
        )

    def test_calculate_flagged(self):
        """
        Test the calculation of flagged status.
        Verifies that the method correctly identifies if any records are flagged.
        """
        # Test with no flagged records
        self.assertFalse(self.processor.calculate_flagged())

        # Test with a flagged record
        self.processor.records.loc[0, "flagged"] = True
        self.assertTrue(self.processor.calculate_flagged())

    def test_calculate_pressure(self):
        """
        Test the calculation of pressure metrics.
        Verifies that the method correctly calculates maximum and minimum pressure.
        """
        max_pressure, min_pressure = self.processor.calculate_pressure()
        self.assertEqual(max_pressure, 1012.0)
        self.assertEqual(min_pressure, 1010.0)

    def test_calculate_wind(self):
        """
        Test the calculation of wind metrics.
        Verifies that the method correctly calculates maximum wind speed,
        maximum wind gust, and average wind direction.
        """
        max_wind_speed, max_wind_gust, avg_wind_direction = (
            self.processor.calculate_wind()
        )
        self.assertEqual(max_wind_speed, 8.0)
        self.assertEqual(max_wind_gust, 13.0)
        # For wind direction, we're calculating vector average
        # so just check it's a reasonable value
        self.assertIsInstance(avg_wind_direction, int)

    def test_calculate_temperature(self):
        """
        Test the calculation of temperature metrics.
        Verifies that the method correctly calculates maximum, minimum,
        and average temperature values.
        """
        max_temp, min_temp, avg_temp = self.processor.calculate_temperature()
        self.assertEqual(max_temp, 16.0)
        self.assertEqual(min_temp, 9.0)
        self.assertEqual(avg_temp, 12.333333333333334)  # average of [10, 15, 12]

    def test_calculate_rain(self):
        """
        Test the calculation of rainfall.
        Verifies that the method correctly determines the total rainfall for the day.
        """
        rain = self.processor.calculate_rain()
        self.assertEqual(rain, 5.0)  # Maximum cumulative rain

    def test_calculate_humidity(self):
        """
        Test the calculation of humidity metrics.
        Verifies that the method correctly calculates maximum, minimum,
        and average humidity values.
        """
        max_humidity, min_humidity, avg_humidity = self.processor.calculate_humidity()
        self.assertEqual(max_humidity, 70.0)
        self.assertEqual(min_humidity, 60.0)
        self.assertEqual(avg_humidity, 65.0)

    def test_generate_record(self):
        """
        Test the record generation functionality.
        Verifies that the processor correctly creates a DailyRecord instance
        with expected values from the processed data.
        """
        record = self.processor.run(True)
        self.assertIsInstance(record, DailyRecord)
        self.assertEqual(record.station_id, "test-station")
        self.assertEqual(record.date, self.date)
        self.assertEqual(record.max_temperature, 16.0)
        self.assertEqual(record.min_temperature, 9.0)
        self.assertEqual(record.rain, 5.0)

    def test_run(self):
        """
        Test the main run method of the processor.
        Verifies normal operation and edge cases such as empty record sets.
        """
        # Test normal operation
        record = self.processor.run(dry_run=True)
        self.assertIsInstance(record, DailyRecord)

        # Test with empty records
        empty_processor = DailyProcessor(
            station=self.station,
            records=pd.DataFrame(),
            date=self.date,
            run_id="test-run-id",
        )
        self.assertIsNone(empty_processor.run(dry_run=True))


if __name__ == "__main__":
    unittest.main()
