"""
Test cases for the monthly_processor.py classes and functions.
"""

import unittest
import datetime

import pandas as pd

from processor.schema import WeatherStation, MonthlyRecord
from processor.processors.monthly_processor import MonthlyProcessor


class TestMonthlyProcessor(unittest.TestCase):
    """
    Test suite for the MonthlyProcessor class that processes monthly weather data.
    """

    def setUp(self):
        """
        Set up test environment before each test method.
        Creates a mock weather station, sample records for a month, and processor instance.
        """
        # Create a mock weather station
        self.station = WeatherStation(
            ws_id="test-station",
            location="Test Station",
            local_timezone="Europe/Madrid",
        )

        # Define the interval for the month
        self.interval = (datetime.date(2024, 4, 1), datetime.date(2024, 4, 30))

        # Create sample daily records for the month
        self.records = pd.DataFrame(
            {
                "id": [101, 102, 103],
                "date": pd.to_datetime(["2024-04-10", "2024-04-15", "2024-04-20"]),
                "max_temperature": [16.0, 18.0, 17.0],
                "min_temperature": [5.0, 7.0, 6.0],
                "avg_temperature": [10.5, 12.5, 11.5],
                "max_pressure": [1015.0, 1018.0, 1016.0],
                "min_pressure": [1005.0, 1008.0, 1006.0],
                "max_wind_gust": [25.0, 30.0, 28.0],
                "rain": [2.0, 5.0, 3.0],
                "max_humidity": [85.0, 90.0, 88.0],
                "min_humidity": [40.0, 45.0, 42.0],
                "avg_humidity": [62.5, 67.5, 65.0],
            }
        )

        # Create processor
        self.processor = MonthlyProcessor(
            station=self.station,
            records=self.records,
            interval=self.interval,
            run_id="test-run-id",
        )

    def test_calculate_temperature(self):
        """
        Test the calculation of temperature metrics for monthly data.
        Verifies that the method correctly calculates maximum, minimum,
        and average temperature values across all days in the month.
        """
        result = self.processor.calculate_temperature()
        max_max_temp, min_min_temp, avg_avg_temp, avg_max_temp, avg_min_temp = result

        self.assertEqual(max_max_temp, 18.0)
        self.assertEqual(min_min_temp, 5.0)
        self.assertEqual(avg_avg_temp, 11.5)  # average of [10.5, 12.5, 11.5]
        self.assertEqual(avg_max_temp, 17.0)  # average of [16.0, 18.0, 17.0]
        self.assertEqual(avg_min_temp, 6.0)  # average of [5.0, 7.0, 6.0]

    def test_calculate_wind(self):
        """
        Test the calculation of wind metrics for monthly data.
        Verifies that the method correctly calculates maximum wind gust
        and average maximum wind gust across all days in the month.
        """
        max_max_wind_gust, avg_max_wind_gust = self.processor.calculate_wind()
        self.assertEqual(max_max_wind_gust, 30.0)
        self.assertEqual(avg_max_wind_gust, 27.67)  # Rounded average of [25, 30, 28]

    def test_calculate_pressure(self):
        """
        Test the calculation of pressure metrics for monthly data.
        Verifies that the method correctly calculates maximum, minimum,
        and average pressure values across all days in the month.
        """
        max_max_pressure, min_min_pressure, avg_pressure = (
            self.processor.calculate_pressure()
        )
        self.assertEqual(max_max_pressure, 1018.0)
        self.assertEqual(min_min_pressure, 1005.0)
        self.assertAlmostEqual(
            avg_pressure, 1011.33, places=1
        )  # Rounded average of pressure means

    def test_calculate_rain(self):
        """
        Test the calculation of total rainfall for monthly data.
        Verifies that the method correctly determines the cumulative
        rainfall across all days in the month.
        """
        total_rain = self.processor.calculate_rain()
        self.assertEqual(total_rain, 10.0)  # Sum of [2, 5, 3]

    def test_calculate_humidity(self):
        """
        Test the calculation of humidity metrics for monthly data.
        Verifies that the method correctly calculates maximum, minimum,
        and average humidity values across all days in the month.
        """
        max_max_humidity, min_min_humidity, avg_humidity = (
            self.processor.calculate_humidity()
        )
        self.assertEqual(max_max_humidity, 90.0)
        self.assertEqual(min_min_humidity, 40.0)
        self.assertEqual(avg_humidity, 65.0)  # Rounded average of [62.5, 67.5, 65.0]

    def test_generate_record(self):
        """
        Test the monthly record generation functionality.
        Verifies that the processor correctly creates a MonthlyRecord instance
        with expected values from the processed data.
        """
        record = self.processor.run(True)
        self.assertIsInstance(record, MonthlyRecord)
        self.assertEqual(record.station_id, "test-station")
        self.assertEqual(record.date, self.interval[0])
        self.assertEqual(record.max_max_temperature, 18.0)
        self.assertEqual(record.min_min_temperature, 5.0)
        self.assertEqual(record.cumulative_rainfall, 10.0)

    def test_run(self):
        """
        Test the main run method of the monthly processor.
        Verifies normal operation and edge cases such as empty record sets.
        """
        # Test normal operation
        record = self.processor.run(dry_run=True)
        self.assertIsInstance(record, MonthlyRecord)

        # Test with empty records
        empty_processor = MonthlyProcessor(
            station=self.station,
            records=pd.DataFrame(),
            interval=self.interval,
            run_id="test-run-id",
        )
        self.assertIsNone(empty_processor.run(dry_run=True))


if __name__ == "__main__":
    unittest.main()
