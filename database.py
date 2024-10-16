import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from typing import List, Tuple, Optional
import datetime
import uuid

from schema import WeatherRecord, DailyRecord

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
        
    @classmethod
    def save_daily_record(cls, record: DailyRecord) -> None:
        """Save a daily record to the database."""
        record.id = str(uuid.uuid4())
        
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                "INSERT INTO daily_record (id, station_id, date, high_temperature, low_temperature, high_wind_speed, high_wind_direction, high_pressure, low_pressure, rain, flagged, finished, cook_run_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (record.id, record.station_id, record.date, record.high_temperature, record.low_temperature, record.high_wind_speed, record.high_wind_direction, record.high_pressure, record.low_pressure, record.rain, record.flagged, record.finished, record.cookRunId)
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


def get_all_stations() -> List[Tuple]:
    """Get all active weather stations."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, name, location FROM weather_station WHERE status = 'active'")
        stations = cursor.fetchall()
        return stations
    
def get_single_station(station_id: str) -> Tuple:
    """Get a single weather station by ID."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, name, location FROM weather_station WHERE id = %s AND status = 'active'", (station_id,))
        station = cursor.fetchone()
        return station
    
def get_records_for_station_and_date(station_id: str, date: datetime.date) -> List[Tuple]:
    """Get all weather records for a specific station and date."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "SELECT id, station_id, source_timestamp, temperature, wind_speed, max_wind_speed, wind_direction, rain, humidity, pressure, flagged, gatherer_run_id, cumulative_rain "
            "FROM weather_record "
            "WHERE station_id = %s AND DATE(taken_timestamp) = %s", 
            (station_id, date)
        )
        stations = cursor.fetchall()
        return stations
    