"""
Processor class for weather record processing.
"""

import zoneinfo

import queue
import uuid
from datetime import datetime, timedelta, date, timezone
import logging
import os

import pandas as pd

from processor.logger import config_logger
from processor.builders import DailyBuilder, MonthlyBuilder, BaseBuilder
from processor.database import Database, database_connection
from processor.schema import ProcessorThread
from processor.scheduler import Scheduler


class Processor:
    """Main class for weather record processing."""

    def __init__(
        self,
        db_url: str,
        dry_run: bool,
        process_date: date,
        mode: str,
        process_pending: bool,
        all_stations: bool = False,
        station_id: str = None,
    ):

        self.db_url = db_url
        self.dry_run = dry_run
        self.date = process_date
        self.mode = mode

        if all_stations and station_id is not None:
            raise ValueError("Cannot specify both station_id and all_stations flag.")

        self.stations = (
            self.get_all_stations()
            if all_stations
            else self.get_single_station(station_id)
        )

        self.process_pending = process_pending

        self.run_id = str(uuid.uuid4())
        self.thread = ProcessorThread(
            thread_id=self.run_id,
            thread_timestamp=datetime.now(timezone.utc),
            command=" ".join(os.sys.argv),
            processed_date=self.date,
        )

        self.scheduler = None  # needs db connection, so it's created later
        self.processing_queue = queue.Queue()
        config_logger(debug=self.dry_run)

    def get_all_stations(self) -> list:
        """
        Retrieve all active stations.
        """
        stations = Database.get_all_stations()

        if len(stations) == 0:
            logging.error("No active stations found!")
            return []

        return stations

    def get_single_station(self, station_id: str) -> list:
        """
        Retrieve a single station by its ID.
        """
        station = Database.get_single_station(station_id)

        if station is None:
            logging.error("Station with ID %s not found.", station_id)

            return []
        return [station]

    def fill_up_daily_queue(self):
        """
        Fill up the processing queue with DailyBuilder instances for each station.
        Each station's records are processed for the specified date.
        """
        full_days_intervals = self.scheduler.get_full_day_intervals()

        for tz, interval in full_days_intervals.items():
            date_on_tz = interval[0].astimezone(tz).date()
            stations_for_tz = [
                station for station in self.stations if station.local_timezone == tz
            ]  # to avoid extra DB query

            for station in stations_for_tz:
                records = Database.get_weather_records_for_station_and_interval(
                    station_id=str(station.ws_id),
                    date_from=interval[0],
                    date_to=interval[1],
                )

                if len(records) == 0:
                    logging.warning(
                        "No records found for station %s on date %s",
                        station.location,
                        date_on_tz,
                    )
                    continue

                self.processing_queue.put(
                    DailyBuilder(
                        station=station,
                        records=pd.DataFrame(records),
                        date=date_on_tz,
                        run_id=self.run_id,
                    )
                )

    def fill_up_monthly_queue(self):
        """
        Fill up the processing queue with MonthlyBuilder instances for each station.
        Each station's records are processed for the specified month.
        """
        month_interval = self.scheduler.get_month_interval()

        for station in self.stations:
            records = Database.get_daily_records_for_station_and_interval(
                station_id=str(station.ws_id),
                start_date=month_interval[0],
                end_date=month_interval[1],
            )

            if len(records) == 0:
                logging.warning(
                    "No daily records found for station %s in interval %s-%s",
                    station.location,
                    month_interval[0].date(),
                    month_interval[1].date(),
                )
                continue

            self.processing_queue.put(
                MonthlyBuilder(
                    station=station,
                    records=pd.DataFrame(records),
                    interval=month_interval,
                    run_id=self.run_id,
                )
            )

    def fill_up_queue_with_pending(self):
        """
        Fill up the processing queue the records for stations and dates
        that are pending reprocessing into a monthly record.
        """
        pending_queue_entries = Database.get_monthly_update_queue_items()
        if len(pending_queue_entries) == 0:
            logging.info("No pending records to process.")
            return

        for entry in pending_queue_entries:
            station = Database.get_single_station(entry.station_id)
            if station is None:
                logging.error(
                    "Pending station with ID %s not found. (Entry ID %s)",
                    entry.station_id,
                    entry.id,
                )
                continue

            interval = (
                datetime(
                    entry.year,
                    entry.month,
                    1,
                    0,
                    0,
                    0,
                    0,
                    zoneinfo.ZoneInfo("UTC"),
                ),
                datetime(
                    entry.year,
                    entry.month + 1 if entry.month < 12 else 1,
                    1 if entry.month < 12 else 1,
                    0,
                    0,
                    0,
                    0,
                    zoneinfo.ZoneInfo("UTC"),
                )
                - timedelta(seconds=1),
            )
            records = Database.get_daily_records_for_station_and_interval(
                station_id=str(station.ws_id),
                start_date=interval[0],
                end_date=interval[1],
            )

            if len(records) == 0:
                logging.warning(
                    "No daily records found for station %s in interval %s-%s",
                    station.location,
                    interval[0].date(),
                    interval[1].date(),
                )
                continue
            self.processing_queue.put(
                MonthlyBuilder(
                    station=station,
                    records=pd.DataFrame(records),
                    interval=interval,
                    run_id=self.run_id,
                )
            )
            logging.info(
                "Added pending record for station %s in interval %s-%s",
                station.location,
                interval[0].date(),
                interval[1].date(),
            )
            Database.delete_monthly_update_queue_item(entry.id)
            logging.info("Deleted entry %s from the queue.", entry.id)

    def process_queue(self):
        """Process the records in the queue."""
        while not self.processing_queue.empty():
            processor = self.processing_queue.get()

            if not isinstance(processor, BaseBuilder):
                logging.error("Processor is not of type BaseBuilder.")
                continue

            logging.info(
                "Processing %s (%d records)",
                processor.station.location,
                len(processor.records),
            )

            result = processor.run(self.dry_run)

            if result:
                logging.info("Successfully processed %s", processor.station.ws_id)
            else:
                logging.error("Did not process %s", processor.station.ws_id)

    def run(self):
        """Main entry point for weather record processing."""
        with database_connection(self.db_url):
            self.scheduler = Scheduler(self.date)

            logging.info("Connected to database.")

            if self.dry_run:
                logging.info("Dry run enabled.")
            else:
                logging.warning("Dry run disabled.")

            match self.mode:
                case "daily":
                    self.fill_up_daily_queue()

                case "monthly":
                    self.fill_up_monthly_queue()

            if self.process_pending:
                logging.info("Processing pending records.")
                self.fill_up_queue_with_pending()

            logging.info("Starting processing. Run ID: %s", self.run_id)

            if not self.processing_queue.empty():
                Database.save_processor_thread(
                    self.thread
                )  # creating before the assignment
                logging.info("Saved thread with id %s.", self.thread.thread_id)

                self.process_queue()

            else:
                logging.warning("No records to process.")

            logging.info("Processor done.")
