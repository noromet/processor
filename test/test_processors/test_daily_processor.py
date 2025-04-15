"""
Test cases for the daily_processor.py classes and functions.
"""

import unittest
from datetime import date
import pandas as pd
import numpy as np

from processors.daily_processor import DailyProcessor


class DummyStation:
    """Dummy station class for testing DailyProcessor."""

    def __init__(self, ws_id="station1", local_timezone="UTC"):
        self.ws_id = ws_id
        self.local_timezone = local_timezone


class TestDailyProcessor(unittest.TestCase):
    """Test cases for the DailyProcessor class."""

    def setUp(self):
        """Set up test variables for DailyProcessor tests."""
        self.station = DummyStation()
        self.run_id = "run-123"
        self.process_date = date(2024, 4, 15)

    def make_df(self, **kwargs):
        """Helper to create a DataFrame from keyword arguments."""
        return pd.DataFrame(kwargs)

    # region flagged
    def test_calculate_flagged_all_false(self):
        """Test calculate_flagged returns False when all flagged are False."""
        df = self.make_df(flagged=[False, False, False])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        self.assertFalse(proc.calculate_flagged())

    def test_calculate_flagged_some_true(self):
        """Test calculate_flagged returns True when any flagged is True."""
        df = self.make_df(flagged=[False, True, False])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        self.assertTrue(proc.calculate_flagged())

    def test_calculate_flagged_empty(self):
        """Test calculate_flagged returns True when all flagged are NaN."""
        df = self.make_df(flagged=[np.nan, np.nan])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        self.assertTrue(proc.calculate_flagged())

    # endregion

    # region pressure
    def test_calculate_pressure(self):
        """Test calculate_pressure returns correct max and min pressure."""
        df = self.make_df(pressure=[1010, 1020, 1005, np.nan])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_p, min_p = proc.calculate_pressure()
        self.assertEqual(max_p, 1020.0)
        self.assertEqual(min_p, 1005.0)

    def test_calculate_pressure_empty(self):
        """Test calculate_pressure returns None for empty/NaN input."""
        df = self.make_df(pressure=[np.nan, np.nan])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_p, min_p = proc.calculate_pressure()
        self.assertIsNone(max_p)
        self.assertIsNone(min_p)

    # endregion

    # region wind
    def test_calculate_wind(self):
        """Test calculate_wind returns correct max wind speed, gust, and avg direction."""
        df = self.make_df(
            wind_speed=[2, 5, 3],
            max_wind_speed=[3, 6, 4],
            wind_gust=[7, 8, 6],
            max_wind_gust=[8, 9, 7],
            wind_direction=[0, 90, 180],
        )
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_ws, max_gust, avg_dir = proc.calculate_wind()
        self.assertEqual(max_ws, 6.0)
        self.assertEqual(max_gust, 9.0)
        self.assertIsInstance(avg_dir, int)

    def test_calculate_wind_empty(self):
        """Test calculate_wind returns NaN and None for empty/NaN input."""
        df = self.make_df(
            wind_speed=[np.nan, np.nan],
            max_wind_speed=[np.nan, np.nan],
            wind_gust=[np.nan, np.nan],
            max_wind_gust=[np.nan, np.nan],
            wind_direction=[np.nan, np.nan],
        )
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_ws, max_gust, avg_dir = proc.calculate_wind()
        self.assertTrue(np.isnan(max_ws))
        self.assertTrue(np.isnan(max_gust))
        self.assertIsNone(avg_dir)

    # endregion

    # region temperature
    def test_calculate_temperature(self):
        """Test calculate_temperature returns correct max, min, and avg temperature."""
        df = self.make_df(
            temperature=[10, 15, 20],
            max_temperature=[18, 21, 19],
            min_temperature=[8, 12, 9],
        )
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_t, min_t, avg_t = proc.calculate_temperature()
        self.assertEqual(max_t, 21.0)
        self.assertEqual(min_t, 8.0)
        self.assertEqual(avg_t, 15.0)

    def test_calculate_temperature_missing(self):
        """Test calculate_temperature returns NaN for all-NaN input."""
        df = self.make_df(
            temperature=[np.nan, np.nan],
            max_temperature=[np.nan, np.nan],
            min_temperature=[np.nan, np.nan],
        )
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_t, min_t, avg_t = proc.calculate_temperature()
        self.assertTrue(np.isnan(max_t))
        self.assertTrue(np.isnan(min_t))
        self.assertTrue(np.isnan(avg_t))

    # endregion

    # region rain
    def test_calculate_rain(self):
        """Test calculate_rain returns correct max cumulative rain."""
        df = self.make_df(cumulative_rain=[0.0, 1.2, 2.5, 2.0])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        self.assertEqual(proc.calculate_rain(), 2.5)

    def test_calculate_rain_empty(self):
        """Test calculate_rain returns None for all-NaN input."""
        df = self.make_df(cumulative_rain=[np.nan, np.nan])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        self.assertIsNone(proc.calculate_rain())

    # endregion

    # region humidity
    def test_calculate_humidity(self):
        """Test calculate_humidity returns correct max, min, and avg humidity."""
        df = self.make_df(humidity=[40, 60, 55, 50])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_h, min_h, avg_h = proc.calculate_humidity()
        self.assertEqual(max_h, 60.0)
        self.assertEqual(min_h, 40.0)
        self.assertAlmostEqual(avg_h, 51.25)

    def test_calculate_humidity_empty(self):
        """Test calculate_humidity returns None for all-NaN input."""
        df = self.make_df(humidity=[np.nan, np.nan])
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        max_h, min_h, avg_h = proc.calculate_humidity()
        self.assertIsNone(max_h)
        self.assertIsNone(min_h)
        self.assertIsNone(avg_h)

    # endregion

    # region run
    def test_run_returns_daily_record(self):
        """Test run returns a daily record with correct attributes."""
        df = self.make_df(
            flagged=[False, False],
            pressure=[1010, 1020],
            wind_speed=[2, 5],
            max_wind_speed=[3, 6],
            wind_gust=[7, 8],
            max_wind_gust=[8, 9],
            wind_direction=[0, 90],
            temperature=[10, 15],
            max_temperature=[18, 21],
            min_temperature=[8, 12],
            cumulative_rain=[0.0, 2.5],
            humidity=[40, 60],
        )
        proc = DailyProcessor(self.station, df, self.process_date, self.run_id)
        result = proc.run()
        self.assertEqual(result.station_id, self.station.ws_id)
        self.assertEqual(result.date, self.process_date)
        self.assertTrue(result.finished)
        self.assertEqual(result.cook_run_id, self.run_id)

    # endregion


if __name__ == "__main__":
    unittest.main()
