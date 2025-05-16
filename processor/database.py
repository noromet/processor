"""
Database integration module for the processor module.
"""

from typing import List, Optional
from contextlib import contextmanager
import logging
import datetime
import uuid
import zoneinfo

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor

from processor.schema import (
    DailyRecord,
    MonthlyRecord,
    WeatherStation,
    WeatherRecord,
    ProcessorThread,
    MonthlyUpdateQueue,
)


class CursorFromConnectionFromPool:
    """Context manager for PostgreSQL cursor."""

    def __init__(self):
        self.connection: Optional[_connection] = None
        self.cursor: Optional[_cursor] = None

    def __enter__(self) -> _cursor:
        """Enter the context manager."""
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
        """Exit the context manager."""
        if exception_value:
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)


class Database:
    """Database class for managing PostgreSQL connections."""

    __connection_pool: Optional[pool.SimpleConnectionPool] = None

    @classmethod
    def initialize(cls, connection_string: str) -> None:
        """Initialize the connection pool."""
        cls.__connection_pool = pool.SimpleConnectionPool(1, 10, dsn=connection_string)

    @classmethod
    def get_connection(cls) -> _connection:
        """Get a connection from the pool."""
        if cls.__connection_pool is None:
            raise psycopg2.OperationalError("Connection pool is not initialized.")
        conn = cls.__connection_pool.getconn()
        conn.set_client_encoding("utf8")
        return conn

    @classmethod
    def return_connection(cls, connection: _connection) -> None:
        """Return a connection to the pool."""
        cls.__connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls) -> None:
        """Close all connections in the pool."""
        cls.__connection_pool.closeall()

    @classmethod
    @contextmanager
    def transaction(cls):
        """Context manager for running multiple statements in a single transaction."""
        conn = cls.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            cls.return_connection(conn)

    @classmethod
    def get_all_stations(cls) -> List[WeatherStation]:
        """Get all active weather stations."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                "SELECT id, location, local_timezone FROM weather_station WHERE status = 'active'"
            )
            stations = cursor.fetchall()
            return [
                WeatherStation(
                    id=uuid.UUID(station[0]),
                    location=station[1],
                    local_timezone=zoneinfo.ZoneInfo(station[2]),
                )
                for station in stations
            ]

    @classmethod
    def get_single_station(cls, station_id: str) -> WeatherStation:
        """Get a single weather station by ID."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                "SELECT id, location, local_timezone "
                "FROM weather_station "
                "WHERE id = %s AND status = 'active'",
                (station_id,),
            )
            station = cursor.fetchone()
            if station:
                return WeatherStation(
                    id=uuid.UUID(station[0]),
                    location=station[1],
                    local_timezone=zoneinfo.ZoneInfo(station[2]),
                )
            return None

    @classmethod
    def get_weather_records_for_station_and_interval(
        cls, station_id: str, date_from: datetime.datetime, date_to: datetime.datetime
    ) -> List[WeatherRecord]:
        """Get all weather records for a specific station and date range."""

        # assert both datetimes have the same timezone and tzinfo
        assert date_from.tzinfo == date_to.tzinfo
        assert date_from.tzinfo is not None

        query = """
            SELECT 
                id, 
                station_id, 
                source_timestamp, 
                temperature, 
                wind_speed, 
                max_wind_speed, 
                wind_direction, 
                rain, 
                humidity, 
                pressure, 
                flagged, 
                taken_timestamp, 
                gatherer_thread_id, 
                cumulative_rain, 
                max_temperature, 
                min_temperature, 
                wind_gust, 
                max_wind_gust
            FROM weather_record
            WHERE 
                station_id = %s 
                AND source_timestamp >= %s 
                AND source_timestamp <= %s 
                AND taken_timestamp >= %s 
                AND taken_timestamp <= %s 
            ORDER BY source_timestamp asc
        """

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query, (station_id, date_from, date_to))
            column_names = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()

            weather_records = [
                WeatherRecord(**dict(zip(column_names, row))) for row in records
            ]

            return weather_records

    @classmethod
    def get_daily_records_for_station_and_interval(
        cls, station_id: str, start_date: datetime.date, end_date
    ) -> List[DailyRecord]:
        """Get all daily records for a specific station and date."""

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                SELECT 
                    id as id, 
                    station_id, 
                    date, 
                    max_temperature, 
                    min_temperature, 
                    max_wind_gust,
                    max_wind_speed, 
                    avg_wind_direction, 
                    max_pressure, 
                    min_pressure, 
                    rain, 
                    flagged, 
                    finished, 
                    processor_thread_id, 
                    avg_temperature, 
                    max_humidity, 
                    avg_humidity, 
                    min_humidity,
                    timezone,
                    monthly_record_id,
                    meta_construction_data
                FROM daily_record 
                WHERE station_id = %s AND date >= %s AND date <= %s""",
                (station_id, start_date, end_date),
            )
            records = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            daily_records = [
                DailyRecord(**dict(zip(column_names, row))) for row in records
            ]
            return daily_records

    @classmethod
    def set_monthly_record_id_for_daily_records(
        cls, daily_record_ids: List[str], monthly_record_id: str
    ) -> None:
        """Set the monthly record ID for daily records in a specific date range."""

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                UPDATE daily_record 
                SET monthly_record_id = %s 
                WHERE id IN %s
                """,
                (
                    monthly_record_id,
                    tuple(daily_record_ids),
                ),
            )

    @classmethod
    def save_daily_record(cls, record: DailyRecord) -> str:
        """Save a daily record to the database."""

        record.id = str(uuid.uuid4()) if record.id is None else record.id

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_record (
                    id, station_id, date, max_temperature, min_temperature, max_wind_gust, 
                    max_wind_speed, avg_wind_direction, max_pressure, min_pressure, rain, 
                    flagged, finished, processor_thread_id, avg_temperature, max_humidity, 
                    avg_humidity, min_humidity, timezone, monthly_record_id, meta_construction_data
                )
                SELECT 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM daily_record
                    WHERE station_id = %s AND date = %s AND was_manually_edited = TRUE
                )
                ON CONFLICT (station_id, date) DO UPDATE SET
                    station_id = EXCLUDED.station_id,
                    date = EXCLUDED.date,
                    max_temperature = EXCLUDED.max_temperature,
                    min_temperature = EXCLUDED.min_temperature,
                    max_wind_gust = EXCLUDED.max_wind_gust,
                    max_wind_speed = EXCLUDED.max_wind_speed,
                    avg_wind_direction = EXCLUDED.avg_wind_direction,
                    max_pressure = EXCLUDED.max_pressure,
                    min_pressure = EXCLUDED.min_pressure,
                    rain = EXCLUDED.rain,
                    flagged = EXCLUDED.flagged,
                    finished = EXCLUDED.finished,
                    processor_thread_id = EXCLUDED.processor_thread_id,
                    avg_temperature = EXCLUDED.avg_temperature,
                    max_humidity = EXCLUDED.max_humidity,
                    avg_humidity = EXCLUDED.avg_humidity,
                    min_humidity = EXCLUDED.min_humidity,
                    timezone = EXCLUDED.timezone,
                    monthly_record_id = EXCLUDED.monthly_record_id,
                    meta_construction_data = EXCLUDED.meta_construction_data
                RETURNING id
                """,
                (
                    record.id,
                    record.station_id,
                    record.date,
                    record.max_temperature,
                    record.min_temperature,
                    record.max_wind_gust,
                    record.max_wind_speed,
                    record.avg_wind_direction,
                    record.max_pressure,
                    record.min_pressure,
                    record.rain,
                    record.flagged,
                    record.finished,
                    record.processor_thread_id,
                    record.avg_temperature,
                    record.max_humidity,
                    record.avg_humidity,
                    record.min_humidity,
                    str(record.timezone),
                    record.monthly_record_id,
                    record.meta_construction_data,
                    record.station_id,
                    record.date,
                ),
            )
            fetched_record = cursor.fetchone()

            if fetched_record is None:
                logging.warning(
                    "Existing record was edited manually. Not saving to database."
                )

                return None

            record_id = fetched_record[0]
            record.id = record_id
            logging.info("Saved daily record with ID: %s", record_id)
            return record_id

    @classmethod
    def save_monthly_record(cls, record: MonthlyRecord) -> str:
        """Save a monthly record to the database."""

        record.id = str(uuid.uuid4()) if record.id is None else record.id

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                INSERT INTO monthly_record (
                    id, station_id, date, avg_max_temperature, avg_min_temperature, 
                    avg_avg_temperature, avg_humidity, avg_max_wind_gust, avg_pressure, 
                    max_max_temperature, min_min_temperature, max_max_humidity, 
                    min_min_humidity, max_max_pressure, min_min_pressure, cumulative_rainfall, 
                    processor_thread_id, finished, max_max_wind_gust
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, date) DO UPDATE SET
                    station_id = EXCLUDED.station_id,
                    date = EXCLUDED.date,
                    avg_max_temperature = EXCLUDED.avg_max_temperature,
                    avg_min_temperature = EXCLUDED.avg_min_temperature,
                    avg_avg_temperature = EXCLUDED.avg_avg_temperature,
                    avg_humidity = EXCLUDED.avg_humidity,
                    avg_max_wind_gust = EXCLUDED.avg_max_wind_gust,
                    avg_pressure = EXCLUDED.avg_pressure,
                    max_max_temperature = EXCLUDED.max_max_temperature,
                    min_min_temperature = EXCLUDED.min_min_temperature,
                    max_max_humidity = EXCLUDED.max_max_humidity,
                    min_min_humidity = EXCLUDED.min_min_humidity,
                    max_max_pressure = EXCLUDED.max_max_pressure,
                    min_min_pressure = EXCLUDED.min_min_pressure,
                    cumulative_rainfall = EXCLUDED.cumulative_rainfall,
                    processor_thread_id = EXCLUDED.processor_thread_id,
                    finished = EXCLUDED.finished
                RETURNING id
                """,
                (
                    record.id,
                    record.station_id,
                    record.date,
                    record.avg_max_temperature,
                    record.avg_min_temperature,
                    record.avg_avg_temperature,
                    record.avg_humidity,
                    record.avg_max_wind_gust,
                    record.avg_pressure,
                    record.max_max_temperature,
                    record.min_min_temperature,
                    record.max_max_humidity,
                    record.min_min_humidity,
                    record.max_max_pressure,
                    record.min_min_pressure,
                    record.cumulative_rainfall,
                    record.processor_thread_id,
                    record.finished,
                    record.max_max_wind_gust,
                ),
            )
            record_id = cursor.fetchone()[0]
            record.id = record_id
            logging.info("Saved monthly record with ID: %s", record_id)
            return record_id

    @classmethod
    def get_present_timezones(cls) -> List[str]:
        """Get all unique timezones from the weather stations."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                "SELECT DISTINCT local_timezone FROM weather_station WHERE status = 'active'"
            )

            timezones = [timezone[0] for timezone in cursor.fetchall()]

            return timezones

    @classmethod
    def save_processor_thread(cls, processor_thread: ProcessorThread) -> None:
        """Save a processor thread to the database."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                INSERT INTO processor_thread (id, thread_timestamp,
                    command, processed_date)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    processor_thread.thread_id,
                    processor_thread.thread_timestamp,
                    processor_thread.command,
                    processor_thread.processed_date,
                ),
            )

    @classmethod
    def get_monthly_update_queue_items(cls):
        """Get all items in the monthly update queue."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                SELECT id, station_id, year, month
                FROM monthly_update_queue
                """
            )
            column_names = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            monthly_update_queue_items = [
                MonthlyUpdateQueue(**dict(zip(column_names, row))) for row in records
            ]

            return monthly_update_queue_items

    @classmethod
    def delete_monthly_update_queue_item(cls, item_id: str) -> None:
        """Delete an item from the monthly update queue."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                DELETE FROM monthly_update_queue
                WHERE id = %s
                """,
                (item_id,),
            )


@contextmanager
def database_connection(db_url: str):
    """Context manager for database connection."""
    logging.info("Connecting to database...")
    Database.initialize(db_url)
    try:
        yield
    finally:
        Database.close_all_connections()
        logging.info("Database connections closed.")
