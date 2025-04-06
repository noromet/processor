import datetime
import os
import concurrent.futures
import threading
import json
import pandas as pd
import numpy as np
import uuid
import logging

from schema import WeatherRecord, DailyRecord
from database import save_daily_record, get_weather_records_for_station_and_interval
import pytz


class DailyProcessor:
    run_id = str(uuid.uuid4())  # class variable

    def __init__(
        self,
        station_set: list,
        single_thread: bool = False,
        interval: tuple = None,
        timezone: pytz.timezone = None,
        date: datetime.date = None,
        dry_run: bool = True,
    ):
        self.station_set = list(set(station_set))
        self.single_thread = single_thread
        self.max_threads = int(os.getenv("MAX_THREADS", 4))
        self.dry_run = dry_run
        self.interval = interval
        self.timezone = timezone
        self.date = date

        self.utc_interval = (
            self.timezone.localize(
                datetime.datetime.combine(interval[0], datetime.time.min)
            ).astimezone(pytz.utc),
            self.timezone.localize(
                datetime.datetime.combine(interval[1], datetime.time.max)
            ).astimezone(pytz.utc),
        )

    def run(self):
        logging.info(f"Processing daily data with run ID {self.run_id}")
        logging.info(f"UTC interval: {self.utc_interval[0]} - {self.utc_interval[1]}")

        if len(self.station_set) == 0:
            logging.warning("No active stations found!")
            return

        if self.single_thread:
            self.single_thread_processing(self.station_set)
        else:
            self.multithread_processing(self.station_set)

        logging.info(
            f"Processing complete for run ID {self.run_id} for date {self.interval}"
        )

    def single_thread_processing(self, stations):
        for station in stations:
            self.process_station(station)

    def multithread_processing(self, stations):
        def process_chunk(chunk, chunk_number):
            logging.debug(
                f"Processing chunk {chunk_number} on {threading.current_thread().name}"
            )
            for station in chunk:
                self.process_station(station)

        chunk_size = len(stations) // self.max_threads
        remainder_size = len(stations) % self.max_threads
        chunks = []
        for i in range(self.max_threads):
            start = i * chunk_size
            end = start + chunk_size
            chunks.append(stations[start:end])
        for i in range(remainder_size):
            chunks[i].append(stations[-(i + 1)])

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_threads
        ) as executor:
            for i, chunk in enumerate(chunks):
                executor.submit(process_chunk, chunk, chunk_number=i)

    def process_station(self, station_id: str):  # station is a tuple like id, location
        logging.info(f"Processing station {station_id}")

        try:
            records = get_weather_records_for_station_and_interval(
                station_id, self.utc_interval[0], self.utc_interval[1]
            )  # tuples
            records = [
                WeatherRecord(*record) for record in records
            ]  # list of WeatherRecord objects
            if len(records) == 0 or records is None:
                logging.warning(
                    f"No weather records retrieved for station {station_id}"
                )
                return

            daily_record = build_daily_record(records, self.date, self.timezone)
            daily_record.cook_run_id = self.run_id

            if not self.dry_run:
                save_daily_record(daily_record)
                logging.info(f"Daily record saved for station {station_id}")
            else:
                logging.debug(
                    json.dumps(
                        daily_record.__dict__, indent=4, sort_keys=True, default=str
                    )
                )
                logging.info(
                    f"Dry run enabled, record not saved for station {station_id}"
                )

        except Exception as e:
            logging.error(f"Error processing station {station_id}: {e}")


def build_daily_record(
    records: list[WeatherRecord], date: datetime.datetime, timezone: str
) -> DailyRecord:
    df = pd.DataFrame(
        [
            {
                "id": record.id,
                "station_id": record.station_id,
                "source_timestamp": record.source_timestamp,
                "temperature": record.temperature,
                "wind_speed": record.wind_speed,
                "max_wind_speed": record.max_wind_speed,
                "wind_direction": record.wind_direction,
                "rain": record.rain,
                "cumulative_rain": record.cumulative_rain,
                "humidity": record.humidity,
                "pressure": record.pressure,
                "flagged": record.flagged,
                "taken_timestamp": record.taken_timestamp,
                "gatherer_thread_id": record.gatherer_thread_id,
                "max_temperature": record.max_temperature,
                "min_temperature": record.min_temperature,
                "wind_gust": record.wind_gust,
                "max_wind_gust": record.max_wind_gust,
            }
            for record in records
        ]
    )

    flagged = calculate_flagged(df)
    max_pressure, min_pressure = calculate_pressure(df)
    max_wind_speed, max_wind_gust, avg_wind_direction = calculate_wind(df)
    max_temperature, min_temperature, avg_temperature = calculate_temperature(df)
    max_humidity, min_humidity, avg_humidity = calculate_humidity(df)
    total_rain = calculate_rain(df)

    return DailyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=date,
        max_temperature=max_temperature,
        min_temperature=min_temperature,
        max_wind_gust=max_wind_speed,
        avg_wind_direction=avg_wind_direction,
        max_pressure=max_pressure,
        min_pressure=min_pressure,
        rain=total_rain,
        flagged=flagged,
        finished=True,
        cook_run_id=None,
        avg_temperature=avg_temperature,
        max_humidity=max_humidity,
        avg_humidity=avg_humidity,
        min_humidity=min_humidity,
        timezone=timezone,
    )


def calculate_flagged(df: pd.DataFrame) -> bool:
    df = df.dropna(subset=["flagged"])
    if df.empty:
        return True

    return bool(df["flagged"].any())


def calculate_pressure(df: pd.DataFrame) -> tuple:
    df = df.dropna(subset=["pressure"])
    if df.empty:
        return None, None

    max_pressure = float(df["pressure"].max())
    min_pressure = float(df["pressure"].min())

    return max_pressure, min_pressure

def calculate_wind(df: pd.DataFrame) -> tuple:
    wind_columns = ["wind_speed", "max_wind_speed", "wind_gust", "max_wind_gust"]
    wind_gust_columns = ["max_wind_gust", "wind_gust"]
    max_global_wind_speed = df[wind_columns].max().max()
    max_global_wind_gust = df[wind_gust_columns].max().max()

    wind_speed_column = "wind_speed"
    wind_direction_column = "wind_direction"

    # Remove rows with NaNs in wind direction or wind speed
    valid_data = df[[wind_speed_column, wind_direction_column]].dropna()

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
    avg_wind_direction = np.rad2deg(avg_direction_rad) % 360  # Normalize to [0, 360)

    return max_global_wind_speed, max_global_wind_gust, avg_wind_direction


def calculate_temperature(df: pd.DataFrame) -> tuple:
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


def calculate_rain(df: pd.DataFrame) -> float:
    df = df.dropna(subset=["cumulative_rain"])
    if df.empty:
        return None

    max_cum_rain = float(df["cumulative_rain"].max())

    return max_cum_rain


def calculate_humidity(df: pd.DataFrame) -> tuple:
    df = df.dropna(subset=["humidity"])
    if df.empty:
        return None, None, None

    max_humidity = float(df["humidity"].max())
    min_humidity = float(df["humidity"].min())
    avg_humidity = float(df["humidity"].mean())
    return max_humidity, min_humidity, avg_humidity
