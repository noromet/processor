"""MonthlyRecord Schema"""

import uuid
import datetime
from dataclasses import dataclass


@dataclass
class MonthlyRecord:
    """
    Represents a single monthly weather record,
    which is an aggregation of multiple DailyRecord instances.

    Attributes:
        id (uuid.UUID): Unique identifier for the monthly record.
        station_id (uuid.UUID): Identifier for the weather station.
        date (datetime.date): The date of the record, typically the first day of the month.
        avg_max_temperature (float): Average of maximum temperatures recorded for the month.
        avg_min_temperature (float): Average of minimum temperatures recorded for the month.
        avg_avg_temperature (float): Average of average temperatures recorded for the month.
        avg_humidity (float): Average humidity recorded for the month.
        avg_max_wind_gust (float): Average of maximum wind gusts recorded for the month.
        avg_pressure (float): Average atmospheric pressure recorded for the month.
        max_max_temperature (float): Maximum of maximum temperatures recorded for the month.
        min_min_temperature (float): Minimum of minimum temperatures recorded for the month.
        max_max_humidity (float): Maximum of maximum humidity recorded for the month.
        min_min_humidity (float): Minimum of minimum humidity recorded for the month.
        max_max_pressure (float): Maximum atmospheric pressure recorded for the month.
        max_max_wind_gust (float): Maximum wind gust recorded for the month.
        min_min_pressure (float): Minimum atmospheric pressure recorded for the month.
        cumulative_rainfall (float): Total rainfall accumulated over the month.
        processor_thread_id (uuid.UUID): Identifier of the processor thread
            that handled this record.
        finished (bool): Indicates if the record processing is complete.
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
