"""
Module for processing daily weather station data into daily summary records.
"""

import datetime
import json
import logging

import pandas as pd
import numpy as np

from processor.schema import WeatherStation, DailyRecord
from processor.database import Database
from .base_builder import BaseBuilder


class DailyBuilder(BaseBuilder):
    """
    Processes raw weather station records for a single day into a DailyRecord summary.
    """

    def __init__(
        self,
        station: WeatherStation,
        records: pd.DataFrame,
        date: datetime.date,
        run_id: str,
    ):
        """
        Initialize the DailyBuilder.
        Args:
            station (WeatherStation): The weather station metadata.
            records (pd.DataFrame): DataFrame of raw weather records for the day.
            date (datetime.date): The date for which to process records.
            run_id (str): Unique identifier for this processing run.
        """
        super().__init__(station=station, records=records, run_id=run_id)
        self.date = date

    def _generate_record(self) -> DailyRecord:
        """
        Process the daily records and return a DailyRecord summary.
        Returns:
            DailyRecord: The processed daily summary record.
        """
        flagged = self.calculate_flagged()
        max_pressure, min_pressure = self.calculate_pressure()
        max_wind_speed, max_wind_gust, avg_wind_direction = self.calculate_wind()
        max_temperature, min_temperature, avg_temperature = self.calculate_temperature()
        max_humidity, min_humidity, avg_humidity = self.calculate_humidity()
        total_rain = self.calculate_rain()

        meta_construction_data = json.dumps(
            {
                "source_record_ids": self.records["id"].dropna().astype(str).tolist(),
            }
        )

        return DailyRecord(
            id=None,
            station_id=str(self.station.id),
            date=self.date,
            max_temperature=max_temperature,
            min_temperature=min_temperature,
            max_wind_speed=max_wind_speed,
            max_wind_gust=max_wind_gust,
            avg_wind_direction=avg_wind_direction,
            max_pressure=max_pressure,
            min_pressure=min_pressure,
            rain=total_rain,
            flagged=flagged,
            finished=True,
            processor_thread_id=self.run_id,
            avg_temperature=avg_temperature,
            max_humidity=max_humidity,
            avg_humidity=avg_humidity,
            min_humidity=min_humidity,
            timezone=self.station.local_timezone,
            meta_construction_data=meta_construction_data,
            monthly_record_id=None,
        )

    def _save_record(self, record: DailyRecord) -> None:
        """
        Save the DailyRecord to the database.
        Args:
            record (DailyRecord): The DailyRecord to save.
        """
        Database.save_daily_record(record)

    def run(self, dry_run: bool) -> DailyRecord:
        """
        Process the daily records and save the DailyRecord summary.
        Returns:
            bool: True if processing was successful, otherwise False.
        """
        if len(self.records) == 0:
            return None

        try:
            record = self._generate_record()

            if not dry_run:
                self._save_record(record)
            else:
                logging.debug("Record not saved: %s", str(record.__dict__))

            return record
        except Exception as e:
            logging.error("Error processing daily record: %s", e)
            return None

    def calculate_flagged(self) -> bool:
        """
        Determine if any record in the day is flagged as problematic.
        Returns:
            bool: True if any record is flagged, otherwise False. Returns True if no data.
        """
        df = self.records.dropna(subset=["flagged"])

        if df.empty:
            return True

        return bool(df["flagged"].any())

    def calculate_pressure(self) -> tuple:
        """
        Calculate the maximum and minimum pressure for the day.
        Returns:
            tuple: (max_pressure, min_pressure) or (None, None) if no data.
        """
        df = self.records.dropna(subset=["pressure"])

        if df.empty:
            return None, None

        max_pressure = float(df["pressure"].max())
        min_pressure = float(df["pressure"].min())

        return max_pressure, min_pressure

    def calculate_wind(self) -> tuple:
        """
        Calculate wind statistics: max wind speed, max wind gust, and average wind direction.
        Returns:
            tuple: (max_wind_speed, max_wind_gust, avg_wind_direction)
        """
        df = self.records

        wind_columns = ["wind_speed", "max_wind_speed"]
        wind_gust_columns = ["max_wind_gust", "wind_gust"]
        max_global_wind_speed = df[wind_columns].max().max()
        max_global_wind_gust = df[wind_gust_columns].max().max()

        wind_speed_column = "wind_speed"
        wind_direction_column = "wind_direction"

        # Remove rows with NaNs in wind direction or wind speed
        valid_data = df[[wind_speed_column, wind_direction_column]].dropna()

        if valid_data.empty or valid_data[wind_speed_column].sum() == 0:
            avg_wind_direction = None  # Default value for average wind direction
            return (
                float(max_global_wind_speed),
                float(max_global_wind_gust),
                avg_wind_direction,
            )

        # Convert degrees to radians
        directions_rad = np.deg2rad(valid_data[wind_direction_column])

        # Vector components, weighted by wind speed
        x = np.cos(directions_rad) * valid_data[wind_speed_column]
        y = np.sin(directions_rad) * valid_data[wind_speed_column]

        # Mean vector
        x_mean = x.sum() / valid_data[wind_speed_column].sum()
        y_mean = y.sum() / valid_data[wind_speed_column].sum()

        # Compute average direction in radians, then convert back to degrees
        avg_direction_rad = np.arctan2(y_mean, x_mean)
        avg_wind_direction = (
            np.rad2deg(avg_direction_rad) % 360
        )  # Normalize to [0, 360)

        return (
            float(max_global_wind_speed),
            float(max_global_wind_gust),
            int(avg_wind_direction),
        )

    def calculate_temperature(self) -> tuple:
        """
        Calculate max, min, and average temperature for the day.
        Returns:
            tuple: (max_temperature, min_temperature, avg_temperature)
        """
        df = self.records

        # Calculate max temperature
        max_temperature_max = float(df["max_temperature"].max())
        temperature_max = float(df["temperature"].max())
        if pd.isna(max_temperature_max):
            max_temperature = temperature_max
        elif pd.isna(temperature_max):
            max_temperature = max_temperature_max
        else:
            max_temperature = max(max_temperature_max, temperature_max)

        # Calculate min temperature
        min_temperature_min = float(df["min_temperature"].min())
        temperature_min = float(df["temperature"].min())
        if pd.isna(min_temperature_min):
            min_temperature = temperature_min
        elif pd.isna(temperature_min):
            min_temperature = min_temperature_min
        else:
            min_temperature = min(min_temperature_min, temperature_min)

        # Remove rows with NaN in temperature
        df = df.dropna(subset=["temperature"])

        # Calculate average temperature
        avg_temperature = float(df["temperature"].mean())

        return max_temperature, min_temperature, avg_temperature

    def calculate_rain(self) -> float:
        """
        Calculate the total rain for the day based on cumulative rain values.
        Returns:
            float: The maximum cumulative rain value, or None if no data.
        """
        df = self.records.dropna(subset=["cumulative_rain"])
        if df.empty:
            return None

        max_cum_rain = float(df["cumulative_rain"].max())

        return max_cum_rain

    def calculate_humidity(self) -> tuple:
        """
        Calculate max, min, and average humidity for the day.
        Returns:
            tuple: (max_humidity, min_humidity, avg_humidity) or (None, None, None) if no data.
        """
        df = self.records.dropna(subset=["humidity"])
        if df.empty:
            return None, None, None

        max_humidity = float(df["humidity"].max())
        min_humidity = float(df["humidity"].min())
        avg_humidity = float(df["humidity"].mean())
        return max_humidity, min_humidity, avg_humidity
