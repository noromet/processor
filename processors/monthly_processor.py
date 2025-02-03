import datetime
import os
import concurrent.futures
import threading
import json
import pandas as pd
import uuid
import logging

from schema import DailyRecord, MonthlyRecord
from database import save_monthly_record, get_daily_records_for_station_and_date


class MonthlyProcessor:
    def __init__(
        self,
        station_set: list,
        single_thread: bool = False,
        interval: tuple = None,
        dry_run: bool = True,
    ):
        self.station_set = list(set(station_set))
        self.single_thread = single_thread
        self.date = interval[0]
        self.max_threads = int(os.getenv("MAX_THREADS", 4))
        self.dry_run = dry_run

    def run(self):
        self.run_id = str(uuid.uuid4())

        logging.info(
            f"Processing monthly data for date {self.date} with run ID {self.run_id}"
        )

        if len(self.station_set) == 0:
            logging.warning("No active stations found!")
            return

        if self.single_thread:
            self.single_thread_processing(self.station_set)
        else:
            self.multithread_processing(self.station_set)

        logging.info(
            f"Processing complete for run ID {self.run_id} for date {self.date}"
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
            records = get_daily_records_for_station_and_date(
                station_id, self.date
            )  # tuples
            records = [
                DailyRecord(*record) for record in records
            ]  # list of WeatherRecord objects

            if len(records) == 0 or records is None:
                logging.warning(f"No daily records retrieved for station {station_id}")
                return

            monthly_record = build_monthly_record(records, self.date)
            monthly_record.cook_run_id = self.run_id

            if not self.dry_run:
                save_monthly_record(monthly_record)
                logging.info(f"Monthly record saved for station {station_id}")
            else:
                logging.debug(
                    json.dumps(
                        monthly_record.__dict__, indent=4, sort_keys=True, default=str
                    )
                )
                logging.info(
                    f"Dry run enabled, record not saved for station {station_id}"
                )

        except Exception as e:
            logging.error(f"Error processing station {station_id}: {e}")


def build_monthly_record(
    records: list[DailyRecord], date: datetime.datetime
) -> MonthlyRecord:
    df = pd.DataFrame(
        [
            {
                "id": record.id,
                "station_id": record.station_id,
                "date": record.date,
                "high_temperature": record.high_temperature,
                "low_temperature": record.low_temperature,
                "high_wind_gust": record.high_wind_gust,
                "high_wind_direction": record.high_wind_direction,
                "high_pressure": record.high_pressure,
                "low_pressure": record.low_pressure,
                "rain": record.rain,
                "flagged": record.flagged,
                "finished": record.finished,
                "cook_run_id": record.cook_run_id,
                "avg_temperature": record.avg_temperature,
                "high_humidity": record.high_humidity,
                "avg_humidity": record.avg_humidity,
                "low_humidity": record.low_humidity,
            }
            for record in records
        ]
    )

    (
        high_high_temperature,
        low_low_temperature,
        avg_avg_temperature,
        avg_high_temperature,
        avg_low_temperature,
    ) = calculate_temperature(df)
    high_max_wind_gust, avg_max_wind_gust = calculate_wind(df)
    high_high_pressure, low_low_pressure, avg_pressure = calculate_pressure(df)
    cumulative_rainfall = calculate_rain(df)
    high_high_humidity, low_low_humidity, avg_humidity = calculate_humidity(df)

    return MonthlyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=date,
        avg_high_temperature=avg_high_temperature,
        avg_low_temperature=avg_low_temperature,
        avg_avg_temperature=avg_avg_temperature,
        avg_humidity=avg_humidity,
        avg_max_wind_gust=avg_max_wind_gust,
        avg_pressure=avg_pressure,
        high_high_temperature=high_high_temperature,
        low_low_temperature=low_low_temperature,
        high_high_humidity=high_high_humidity,
        low_low_humidity=low_low_humidity,
        high_max_wind_gust=high_max_wind_gust,
        high_high_pressure=high_high_pressure,
        low_low_pressure=low_low_pressure,
        cumulative_rainfall=cumulative_rainfall,
        cook_run_id=None,
        finished=True,
    )


def calculate_temperature(df: pd.DataFrame) -> tuple:
    df_high = df.dropna(subset=["high_temperature"])
    df_low = df.dropna(subset=["low_temperature"])
    df_avg = df.dropna(subset=["avg_temperature"])

    high_high_temperature = (
        float(round(df_high["high_temperature"].max(), 2))
        if not df_high.empty
        else None
    )
    low_low_temperature = (
        float(round(df_low["low_temperature"].min(), 2)) if not df_low.empty else None
    )
    avg_avg_temperature = (
        float(round(df_avg["avg_temperature"].mean(), 2)) if not df_avg.empty else None
    )
    avg_high_temperature = (
        float(round(df_high["high_temperature"].mean(), 2))
        if not df_high.empty
        else None
    )
    avg_low_temperature = (
        float(round(df_low["low_temperature"].mean(), 2)) if not df_low.empty else None
    )

    return (
        high_high_temperature,
        low_low_temperature,
        avg_avg_temperature,
        avg_high_temperature,
        avg_low_temperature,
    )


def calculate_wind(df: pd.DataFrame) -> tuple:
    df_high = df.dropna(subset=["high_wind_gust"])
    high_max_wind_gust = (
        float(round(df_high["high_wind_gust"].max(), 2)) if not df_high.empty else None
    )
    avg_max_wind_gust = (
        float(round(df_high["high_wind_gust"].mean(), 2)) if not df_high.empty else None
    )

    return high_max_wind_gust, avg_max_wind_gust


def calculate_pressure(df: pd.DataFrame) -> tuple:
    df_high = df.dropna(subset=["high_pressure"])
    df_low = df.dropna(subset=["low_pressure"])

    high_high_pressure = (
        float(round(df_high["high_pressure"].max(), 2)) if not df_high.empty else None
    )
    low_low_pressure = (
        float(round(df_low["low_pressure"].min(), 2)) if not df_low.empty else None
    )

    if not df_high.empty and not df_low.empty:
        avg_pressure = float(
            round(
                pd.concat([df_high["high_pressure"], df_low["low_pressure"]]).mean(), 2
            )
        )
    else:
        avg_pressure = None

    return high_high_pressure, low_low_pressure, avg_pressure


def calculate_rain(df: pd.DataFrame) -> float:
    df_rain = df.dropna(subset=["rain"])
    cumulative_rainfall = (
        float(round(df_rain["rain"].sum(), 2)) if not df_rain.empty else None
    )

    return cumulative_rainfall


def calculate_humidity(df: pd.DataFrame) -> tuple:
    df_high = df.dropna(subset=["high_humidity"])
    df_low = df.dropna(subset=["low_humidity"])
    df_avg = df.dropna(subset=["avg_humidity"])

    high_high_humidity = (
        float(round(df_high["high_humidity"].max(), 2)) if not df_high.empty else None
    )
    low_low_humidity = (
        float(round(df_low["low_humidity"].min(), 2)) if not df_low.empty else None
    )
    avg_humidity = (
        float(round(df_avg["avg_humidity"].mean(), 2)) if not df_avg.empty else None
    )

    return high_high_humidity, low_low_humidity, avg_humidity
