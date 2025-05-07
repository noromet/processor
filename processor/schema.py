"""
Schema for the weather data processing module.
"""

import datetime
import uuid
import zoneinfo
from dataclasses import dataclass


@dataclass
class WeatherStation:
    """
    Represents a weather station.
    """

    id: uuid.UUID
    location: str
    local_timezone: zoneinfo.ZoneInfo


@dataclass
class WeatherRecord:
    """
    Represents a single weather record from an exact point in time.
    """

    id: uuid.UUID
    station_id: uuid.UUID
    source_timestamp: datetime.datetime
    temperature: float
    wind_speed: float
    max_wind_speed: float
    wind_direction: float
    rain: float
    humidity: float
    pressure: float
    flagged: bool
    taken_timestamp: datetime.datetime
    gatherer_thread_id: uuid.UUID
    cumulative_rain: float
    max_temperature: float
    min_temperature: float
    wind_gust: float
    max_wind_gust: float


@dataclass
class DailyRecord:
    """
    Represents a single daily weather record,
    which is an aggregation of multiple WeatherRecord instances.
    """

    id: uuid.UUID
    station_id: uuid.UUID
    date: datetime.date
    max_temperature: float
    min_temperature: float
    max_wind_gust: float
    max_wind_speed: float
    avg_wind_direction: float
    max_pressure: float
    min_pressure: float
    rain: float
    flagged: bool
    finished: bool
    processor_thread_id: uuid.UUID
    avg_temperature: float
    max_humidity: float
    avg_humidity: float
    min_humidity: float
    timezone: zoneinfo.ZoneInfo
    monthly_record_id: uuid.UUID
    meta_construction_data: str


@dataclass
class MonthlyRecord:
    """
    Represents a single monthly weather record,
    which is an aggregation of multiple DailyRecord instances.
    """

    id: uuid.UUID
    station_id: uuid.UUID
    date: datetime.date
    avg_max_temperature: float
    avg_min_temperature: float
    avg_avg_temperature: float
    avg_humidity: float
    avg_max_wind_gust: float
    avg_pressure: float
    max_max_temperature: float
    min_min_temperature: float
    max_max_humidity: float
    min_min_humidity: float
    max_max_pressure: float
    max_max_wind_gust: float
    min_min_pressure: float
    cumulative_rainfall: float
    processor_thread_id: uuid.UUID
    finished: bool = True


@dataclass
class ProcessorThread:
    """
    Represents a thread that processes weather data.
    """

    thread_id: uuid.UUID
    thread_timestamp: datetime.datetime
    command: str
    processed_date: datetime.date


@dataclass
class MonthlyUpdateQueue:
    """
    Represents an entry in the monthly update queue.
    """

    id: uuid.UUID
    station_id: uuid.UUID
    year: int
    month: int
