"""WeatherRecord Schema"""

import uuid
import datetime
from dataclasses import dataclass


@dataclass
class WeatherRecord:
    """
    Represents a single weather record from an exact point in time.

    Attributes:
        id (uuid.UUID): Unique identifier for the weather record.
        station_id (uuid.UUID): Identifier for the weather station.
        source_timestamp (datetime.datetime): Timestamp when the data was sourced.
        temperature (float): Temperature at the time of the record.
        wind_speed (float): Wind speed at the time of the record.
        max_wind_speed (float): Maximum wind speed recorded.
        wind_direction (float): Wind direction at the time of the record.
        rain (float): Rainfall amount at the time of the record.
        humidity (float): Humidity level at the time of the record.
        pressure (float): Atmospheric pressure at the time of the record.
        flagged (bool): Indicates if the record has been flagged as suspicious.
        taken_timestamp (datetime.datetime): Timestamp when the data was taken.
        gatherer_thread_id (uuid.UUID): Identifier of the thread that gathered this data.
        cumulative_rain (float): Total rainfall accumulated up to this point.
        max_temperature (float): Maximum temperature recorded up to this point.
        min_temperature (float): Minimum temperature recorded up to this point.
        wind_gust (float): Wind gust speed at the time of the record.
        max_wind_gust (float): Maximum wind gust speed recorded up to this point.
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
