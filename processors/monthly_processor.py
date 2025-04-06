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
            ]  # list of DailyRecord objects

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
                "max_temperature": record.max_temperature,
                "min_temperature": record.min_temperature,
                "max_wind_gust": record.max_wind_gust,
                "max_wind_speed": record.max_wind_speed,
                "avg_wind_direction": record.avg_wind_direction,
                "max_pressure": record.max_pressure,
                "min_pressure": record.min_pressure,
                "rain": record.rain,
                "flagged": record.flagged,
                "finished": record.finished,
                "cook_run_id": record.cook_run_id,
                "avg_temperature": record.avg_temperature,
                "max_humidity": record.max_humidity,
                "avg_humidity": record.avg_humidity,
                "min_humidity": record.min_humidity,
            }
            for record in records
        ]
    )

    (
        max_max_temperature,
        min_min_temperature,
        avg_avg_temperature,
        avg_max_temperature,
        avg_min_temperature,
    ) = calculate_temperature(df)
    
    max_max_wind_gust, avg_max_wind_gust = calculate_wind(df)
    max_max_pressure, min_min_pressure, avg_pressure = calculate_pressure(df)
    cumulative_rainfall = calculate_rain(df)
    max_max_humidity, min_min_humidity, avg_humidity = calculate_humidity(df)

    return MonthlyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=date,
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
        cook_run_id=None,
        finished=True,
    )


def calculate_temperature(df: pd.DataFrame) -> tuple:
    df_max = df.dropna(subset=["max_temperature"])
    df_min = df.dropna(subset=["min_temperature"])
    df_avg = df.dropna(subset=["avg_temperature"])

    max_max_temperature = (
        float(round(df_max["max_temperature"].max(), 2))
        if not df_max.empty
        else None
    )
    min_min_temperature = (
        float(round(df_min["min_temperature"].min(), 2)) if not df_min.empty else None
    )
    avg_avg_temperature = (
        float(round(df_avg["avg_temperature"].mean(), 2)) if not df_avg.empty else None
    )
    avg_max_temperature = (
        float(round(df_max["max_temperature"].mean(), 2))
        if not df_max.empty
        else None
    )
    avg_min_temperature = (
        float(round(df_min["min_temperature"].mean(), 2)) if not df_min.empty else None
    )

    return (
        max_max_temperature,
        min_min_temperature,
        avg_avg_temperature,
        avg_max_temperature,
        avg_min_temperature,
    )


def calculate_wind(df: pd.DataFrame) -> tuple:
    df_max = df.dropna(subset=["max_wind_gust"])
    max_max_wind_gust = (
        float(round(df_max["max_wind_gust"].max(), 2)) if not df_max.empty else None
    )
    avg_max_wind_gust = (
        float(round(df_max["max_wind_gust"].mean(), 2)) if not df_max.empty else None
    )

    return max_max_wind_gust, avg_max_wind_gust


def calculate_pressure(df: pd.DataFrame) -> tuple:
    df_max = df.dropna(subset=["max_pressure"])
    df_min = df.dropna(subset=["min_pressure"])

    max_max_pressure = (
        float(round(df_max["max_pressure"].max(), 2)) if not df_max.empty else None
    )
    min_min_pressure = (
        float(round(df_min["min_pressure"].min(), 2)) if not df_min.empty else None
    )

    if not df_max.empty and not df_min.empty:
        avg_pressure = float(
            round(
                pd.concat([df_max["max_pressure"], df_min["min_pressure"]]).mean(), 2
            )
        )
    else:
        avg_pressure = None

    return max_max_pressure, min_min_pressure, avg_pressure


def calculate_rain(df: pd.DataFrame) -> float:
    df_rain = df.dropna(subset=["rain"])
    cumulative_rainfall = (
        float(round(df_rain["rain"].sum(), 2)) if not df_rain.empty else None
    )

    return cumulative_rainfall


def calculate_humidity(df: pd.DataFrame) -> tuple:
    df_max = df.dropna(subset=["max_humidity"])
    df_min = df.dropna(subset=["min_humidity"])
    df_avg = df.dropna(subset=["avg_humidity"])

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
