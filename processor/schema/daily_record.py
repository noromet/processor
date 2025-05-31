"""DailyRecord Schema"""

import uuid
import zoneinfo
import datetime
from dataclasses import dataclass


@dataclass
class DailyRecord:
    """
    Represents a single daily weather record,
    which is an aggregation of multiple WeatherRecord instances.

    Attributes:
        id (uuid.UUID): Unique identifier for the daily record.
        station_id (uuid.UUID): Identifier for the weather station.
        date (datetime.date): The date of the record.
        max_temperature (float): Maximum temperature recorded for the day.
        min_temperature (float): Minimum temperature recorded for the day.
        max_wind_gust (float): Maximum wind gust recorded for the day.
        max_wind_speed (float): Maximum wind speed recorded for the day.
        avg_wind_direction (float): Average wind direction for the day.
        max_pressure (float): Maximum atmospheric pressure recorded for the day.
        min_pressure (float): Minimum atmospheric pressure recorded for the day.
        rain (float): Total rainfall recorded for the day.
        flagged (bool): Indicates if the record has been flagged as suspicious.
        finished (bool): Indicates if the record processing is complete.
        processor_thread_id (uuid.UUID): Identifier of the
            processor thread that created this record.
        avg_temperature (float): Average temperature for the day.
        max_humidity (float): Maximum humidity recorded for the day.
        avg_humidity (float): Average humidity for the day.
        min_humidity (float): Minimum humidity recorded for the day.
        timezone (zoneinfo.ZoneInfo): Timezone of the weather station.
        monthly_record_id (uuid.UUID): Identifier of the associated
            monthly record, if it exists.
        meta_construction_data (str): Additional metadata related to construction data, if any.
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
