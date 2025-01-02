import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from typing import List, Tuple, Optional
import datetime
import uuid

from schema import DailyRecord, MonthlyRecord

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
        conn.set_client_encoding('utf8')
        return conn

    @classmethod
    def return_connection(cls, connection: _connection) -> None:
        """Return a connection to the pool."""
        cls.__connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls) -> None:
        """Close all connections in the pool."""
        cls.__connection_pool.closeall()
        

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


def get_all_stations() -> List[Tuple]:
    """Get all active weather stations."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, location FROM weather_station WHERE status = 'active'")
        stations = cursor.fetchall()
        return stations
    
def get_single_station(station_id: str) -> Tuple:
    """Get a single weather station by ID."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, location FROM weather_station WHERE id = %s AND status = 'active'", (station_id,))
        station = cursor.fetchone()
        return station
    
def get_weather_records_for_station_and_date(station_id: str, date: datetime.date) -> List[Tuple]:
    """Get all weather records for a specific station and date."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "SELECT id, station_id, source_timestamp, temperature, wind_speed, max_wind_speed, wind_direction, rain, humidity, pressure, flagged, gatherer_thread_id, cumulative_rain, max_temp, min_temp, max_wind_gust "
            "FROM weather_record "
            "WHERE station_id = %s AND DATE(taken_timestamp) = %s", 
            (station_id, date)
        )
        records = cursor.fetchall()
        return records
    
def get_daily_records_for_station_and_date(station_id: str, date: datetime.date) -> List[Tuple]:
    """Get all daily records for a specific station and date."""

    #first day of the month
    date_from = date.replace(day=1)
    
    #last day of the month
    date_to = date.replace(day=28) + datetime.timedelta(days=4)
    date_to = date_to - datetime.timedelta(days=date_to.day)

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "SELECT id, station_id, date, high_temperature, low_temperature, high_wind_gust, high_wind_direction, high_pressure, low_pressure, rain, flagged, finished, cook_run_id, avg_temperature, high_humidity, avg_humidity, low_humidity "
            "FROM daily_record "
            "WHERE station_id = %s AND date >= %s AND date <= %s", 
            (station_id, date_from, date_to)
        )
        records = cursor.fetchall()
        return records
    
def save_daily_record(record: DailyRecord) -> None:
    """Save a daily record to the database."""
    record.id = str(uuid.uuid4())
    
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "INSERT INTO daily_record (id, station_id, date, high_temperature, low_temperature, high_wind_gust, high_wind_direction, high_pressure, low_pressure, rain, flagged, finished, cook_run_id, avg_temperature, high_humidity, avg_humidity, low_humidity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (record.id, record.station_id, record.date, record.high_temperature, record.low_temperature, record.high_wind_gust, record.high_wind_direction, record.high_pressure, record.low_pressure, record.rain, record.flagged, record.finished, record.cookRunId, record.avg_temperature, record.high_humidity, record.avg_humidity, record.low_humidity)
        )

def save_monthly_record(record: MonthlyRecord) -> None:
    """Save a monthly record to the database."""
    record.id = str(uuid.uuid4())

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "INSERT INTO monthly_record (id, station_id, date, avg_high_temperature, avg_low_temperature, avg_avg_temperature, avg_humidity, avg_max_wind_gust, avg_pressure, high_high_temperature, low_low_temperature, high_high_humidity, low_low_humidity, high_max_wind_gust, high_high_pressure, low_low_pressure, cumulative_rainfall, cook_run_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (record.id, record.station_id, record.date, record.avg_high_temperature, record.avg_low_temperature, record.avg_avg_temperature, record.avg_humidity, record.avg_max_wind_gust, record.avg_pressure, record.high_high_temperature, record.low_low_temperature, record.high_high_humidity, record.low_low_humidity, record.high_max_wind_gust, record.high_high_pressure, record.low_low_pressure, record.cumulative_rainfall, record.cook_run_id)
        )