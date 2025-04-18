"""
Test cases for the monthly_processor.py classes and functions.
"""

from datetime import datetime
import unittest

import pandas as pd
import numpy as np

from processors.monthly_processor import MonthlyProcessor


class DummyStation:
    """Dummy station class for testing MonthlyProcessor."""

    def __init__(self, ws_id="station1", local_timezone="UTC"):
        self.ws_id = ws_id
        self.local_timezone = local_timezone
        self.location = "Test Location"


class TestMonthlyProcessor(unittest.TestCase):
    """Test cases for the MonthlyProcessor class."""

    def setUp(self):
        """Set up test variables for MonthlyProcessor tests."""
        self.station = DummyStation()
        self.run_id = "run-456"
        self.interval = (
            datetime(2024, 4, 1),
            datetime(2024, 4, 30, 23, 59, 59),
        )

    def make_df(self, **kwargs):
        """Helper to create a DataFrame from keyword arguments."""
        return pd.DataFrame(kwargs)

    # region temperature
    def test_calculate_temperature(self):
        """Test calculate_temperature returns correct max, min, and avg temperature."""
        df = self.make_df(
            max_temperature=[20, 22, 21],
            min_temperature=[10, 12, 11],
            avg_temperature=[15, 17, 16],
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_max, min_min, avg_avg, avg_max, avg_min = proc.calculate_temperature()
        self.assertEqual(max_max, 22.0)
        self.assertEqual(min_min, 10.0)
        self.assertEqual(avg_avg, 16.0)
        self.assertEqual(avg_max, 21.0)
        self.assertEqual(avg_min, 11.0)

    def test_calculate_temperature_empty(self):
        """Test calculate_temperature returns None for all-NaN input."""
        df = self.make_df(
            max_temperature=[np.nan, np.nan],
            min_temperature=[np.nan, np.nan],
            avg_temperature=[np.nan, np.nan],
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_max, min_min, avg_avg, avg_max, avg_min = proc.calculate_temperature()
        self.assertIsNone(max_max)
        self.assertIsNone(min_min)
        self.assertIsNone(avg_avg)
        self.assertIsNone(avg_max)
        self.assertIsNone(avg_min)

    # endregion

    # region wind
    def test_calculate_wind(self):
        """Test calculate_wind returns correct max and avg wind gust."""
        df = self.make_df(max_wind_gust=[10, 15, 12])
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_gust, avg_gust = proc.calculate_wind()
        self.assertEqual(max_gust, 15.0)
        self.assertEqual(avg_gust, 12.33)

    def test_calculate_wind_empty(self):
        """Test calculate_wind returns None for all-NaN input."""
        df = self.make_df(max_wind_gust=[np.nan, np.nan])
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_gust, avg_gust = proc.calculate_wind()
        self.assertIsNone(max_gust)
        self.assertIsNone(avg_gust)

    # endregion

    # region pressure
    def test_calculate_pressure(self):
        """Test calculate_pressure returns correct max, min, and avg pressure."""
        df = self.make_df(
            max_pressure=[1020, 1015, 1010], min_pressure=[1000, 1005, 1010]
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_p, min_p, avg_p = proc.calculate_pressure()
        self.assertEqual(max_p, 1020.0)
        self.assertEqual(min_p, 1000.0)
        self.assertEqual(avg_p, 1010.0)

    def test_calculate_pressure_empty(self):
        """Test calculate_pressure returns None for all-NaN input."""
        df = self.make_df(max_pressure=[np.nan, np.nan], min_pressure=[np.nan, np.nan])
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_p, min_p, avg_p = proc.calculate_pressure()
        self.assertIsNone(max_p)
        self.assertIsNone(min_p)
        self.assertIsNone(avg_p)

    # endregion

    # region rain
    def test_calculate_rain(self):
        """Test calculate_rain returns correct total rain."""
        df = self.make_df(rain=[0.0, 1.2, 2.5, 2.0])
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        self.assertEqual(proc.calculate_rain(), 5.7)

    def test_calculate_rain_empty(self):
        """Test calculate_rain returns None for all-NaN input."""
        df = self.make_df(rain=[np.nan, np.nan])
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        self.assertIsNone(proc.calculate_rain())

    # endregion

    # region humidity
    def test_calculate_humidity(self):
        """Test calculate_humidity returns correct max, min, and avg humidity."""
        df = self.make_df(
            max_humidity=[80, 85, 90],
            min_humidity=[40, 45, 50],
            avg_humidity=[60, 65, 70],
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_h, min_h, avg_h = proc.calculate_humidity()
        self.assertEqual(max_h, 90.0)
        self.assertEqual(min_h, 40.0)
        self.assertEqual(avg_h, 65.0)

    def test_calculate_humidity_empty(self):
        """Test calculate_humidity returns None for all-NaN input."""
        df = self.make_df(
            max_humidity=[np.nan, np.nan],
            min_humidity=[np.nan, np.nan],
            avg_humidity=[np.nan, np.nan],
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        max_h, min_h, avg_h = proc.calculate_humidity()
        self.assertIsNone(max_h)
        self.assertIsNone(min_h)
        self.assertIsNone(avg_h)

    # endregion

    # region run
    def test_run_returns_monthly_record(self):
        """Test run returns a monthly record with correct attributes."""
        df = self.make_df(
            max_temperature=[20, 22],
            min_temperature=[10, 12],
            avg_temperature=[15, 17],
            max_wind_gust=[10, 15],
            max_pressure=[1020, 1015],
            min_pressure=[1005, 1008],
            rain=[1.0, 2.0],
            max_humidity=[80, 85],
            min_humidity=[40, 45],
            avg_humidity=[60, 65],
        )
        proc = MonthlyProcessor(self.station, df, self.interval, self.run_id)
        result = proc.run()
        self.assertEqual(result.station_id, self.station.ws_id)
        self.assertEqual(result.date, self.interval[0])
        self.assertTrue(result.finished)
        self.assertEqual(result.processor_thread_id, self.run_id)

    # endregion


if __name__ == "__main__":
    unittest.main()
