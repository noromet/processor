"""
Monthly weather data processor for aggregating and summarizing weather station records.
"""

import logging

import pandas as pd

from processor.schema import MonthlyRecord, WeatherStation
from processor.database import Database
from .base_builder import BaseBuilder


class MonthlyBuilder(BaseBuilder):
    """
    Processes a month's worth of weather data for a given weather station and interval.
    """

    def __init__(
        self,
        station: WeatherStation,
        records: pd.DataFrame,
        interval: tuple,
        run_id: str,
    ):
        """
        Initialize the MonthlyBuilder.

        Args:
            station (WeatherStation): The weather station object.
            records (pd.DataFrame): DataFrame containing daily weather records.
            interval (tuple): Tuple representing the date interval (start, end).
            run_id (str): Unique identifier for the processing run.
        """
        super().__init__(station=station, records=records, run_id=run_id)
        self.interval = interval

    def _generate_record(self) -> MonthlyRecord:
        """
        Generate the MonthlyRecord summary from the records.

        Returns:
            MonthlyRecord: Aggregated monthly weather data.
        """
        (
            max_max_temperature,
            min_min_temperature,
            avg_avg_temperature,
            avg_max_temperature,
            avg_min_temperature,
        ) = self.calculate_temperature()

        max_max_wind_gust, avg_max_wind_gust = self.calculate_wind()
        max_max_pressure, min_min_pressure, avg_pressure = self.calculate_pressure()
        cumulative_rainfall = self.calculate_rain()
        max_max_humidity, min_min_humidity, avg_humidity = self.calculate_humidity()

        return MonthlyRecord(
            id=None,
            station_id=str(self.station.ws_id),
            date=self.interval[0],
            avg_max_temperature=avg_max_temperature,
            avg_min_temperature=avg_min_temperature,
            avg_avg_temperature=avg_avg_temperature,
            avg_humidity=avg_humidity,
            avg_max_wind_gust=avg_max_wind_gust,
            avg_pressure=avg_pressure,
            max_max_temperature=max_max_temperature,
            min_min_temperature=min_min_temperature,
            max_max_humidity=max_max_humidity,
            min_min_humidity=min_min_humidity,
            max_max_wind_gust=max_max_wind_gust,
            max_max_pressure=max_max_pressure,
            min_min_pressure=min_min_pressure,
            cumulative_rainfall=cumulative_rainfall,
            processor_thread_id=self.run_id,
            finished=True,
        )

    def _save_record(self, record: MonthlyRecord) -> None:
        """
        Save the MonthlyRecord to the database.

        Args:
            record (MonthlyRecord): The MonthlyRecord to save.
        """
        record_ids = self.records["id"].dropna().astype(str).tolist()

        with Database.transaction():  # otherwise, FK is broken
            monthly_record_id = Database.save_monthly_record(record)

            Database.set_monthly_record_id_for_daily_records(
                daily_record_ids=record_ids,
                monthly_record_id=monthly_record_id,
            )

    def run(self, dry_run: bool) -> MonthlyRecord:
        """
        Process the monthly records and save the MonthlyRecord summary.

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
            logging.error("Error processing monthly record: %s", e)
            return None

    def calculate_temperature(self) -> tuple:
        """
        Calculate temperature statistics for the month.

        Returns:
            tuple: (max_max_temperature, min_min_temperature,
                avg_avg_temperature, avg_max_temperature, avg_min_temperature)
        """
        df_max = self.records.dropna(subset=["max_temperature"])
        df_min = self.records.dropna(subset=["min_temperature"])
        df_avg = self.records.dropna(subset=["avg_temperature"])

        max_max_temperature = (
            float(round(df_max["max_temperature"].max(), 2))
            if not df_max.empty
            else None
        )
        min_min_temperature = (
            float(round(df_min["min_temperature"].min(), 2))
            if not df_min.empty
            else None
        )
        avg_avg_temperature = (
            float(round(df_avg["avg_temperature"].mean(), 2))
            if not df_avg.empty
            else None
        )
        avg_max_temperature = (
            float(round(df_max["max_temperature"].mean(), 2))
            if not df_max.empty
            else None
        )
        avg_min_temperature = (
            float(round(df_min["min_temperature"].mean(), 2))
            if not df_min.empty
            else None
        )

        return (
            max_max_temperature,
            min_min_temperature,
            avg_avg_temperature,
            avg_max_temperature,
            avg_min_temperature,
        )

    def calculate_wind(self) -> tuple:
        """
        Calculate wind gust statistics for the month.

        Returns:
            tuple: (max_max_wind_gust, avg_max_wind_gust)
        """
        df_max = self.records.dropna(subset=["max_wind_gust"])
        max_max_wind_gust = (
            float(round(df_max["max_wind_gust"].max(), 2)) if not df_max.empty else None
        )
        avg_max_wind_gust = (
            float(round(df_max["max_wind_gust"].mean(), 2))
            if not df_max.empty
            else None
        )

        return max_max_wind_gust, avg_max_wind_gust

    def calculate_pressure(self) -> tuple:
        """
        Calculate pressure statistics for the month.

        Returns:
            tuple: (max_max_pressure, min_min_pressure, avg_pressure)
        """
        df_max = self.records.dropna(subset=["max_pressure"])
        df_min = self.records.dropna(subset=["min_pressure"])

        max_max_pressure = (
            float(round(df_max["max_pressure"].max(), 2)) if not df_max.empty else None
        )
        min_min_pressure = (
            float(round(df_min["min_pressure"].min(), 2)) if not df_min.empty else None
        )

        df_both = self.records.dropna(subset=["max_pressure", "min_pressure"])
        if not df_both.empty:
            avg_pressure = float(
                round(
                    ((df_both["max_pressure"] + df_both["min_pressure"]) / 2).mean(), 2
                )
            )
        else:
            avg_pressure = None

        return max_max_pressure, min_min_pressure, avg_pressure

    def calculate_rain(self) -> float:
        """
        Calculate cumulative rainfall for the month.

        Returns:
            float: Total rainfall for the month, or None if no data.
        """
        df_rain = self.records.dropna(subset=["rain"])
        cumulative_rainfall = (
            float(round(df_rain["rain"].sum(), 2)) if not df_rain.empty else None
        )

        return cumulative_rainfall

    def calculate_humidity(self) -> tuple:
        """
        Calculate humidity statistics for the month.

        Returns:
            tuple: (max_max_humidity, min_min_humidity, avg_humidity)
        """
        df_max = self.records.dropna(subset=["max_humidity"])
        df_min = self.records.dropna(subset=["min_humidity"])
        df_avg = self.records.dropna(subset=["avg_humidity"])

        max_max_humidity = (
            float(round(df_max["max_humidity"].max(), 2)) if not df_max.empty else None
        )
        min_min_humidity = (
            float(round(df_min["min_humidity"].min(), 2)) if not df_min.empty else None
        )
        avg_humidity = (
            float(round(df_avg["avg_humidity"].mean(), 2)) if not df_avg.empty else None
        )

        return max_max_humidity, min_min_humidity, avg_humidity
