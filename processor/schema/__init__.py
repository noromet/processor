"""
Module containing the schema definitions for the data processing component.
"""

from .daily_record import DailyRecord
from .monthly_record import MonthlyRecord
from .weather_station import WeatherStation
from .weather_record import WeatherRecord
from .monthly_update_queue import MonthlyUpdateQueue
from .processor_thread import ProcessorThread

__all__ = [
    "DailyRecord",
    "MonthlyRecord",
    "WeatherStation",
    "WeatherRecord",
    "MonthlyUpdateQueue",
    "ProcessorThread",
]
