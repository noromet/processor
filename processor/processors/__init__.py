"""
Processor module for weather records.
"""

from .daily_processor import DailyProcessor
from .monthly_processor import MonthlyProcessor
from .base_processor import BaseProcessor

__all__ = [
    "DailyProcessor",
    "MonthlyProcessor",
    "BaseProcessor",
]
