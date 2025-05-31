"""
Base class for all builders.
"""

from abc import ABC, abstractmethod

import pandas as pd

from processor.schema import DailyRecord, MonthlyRecord, WeatherStation


class BaseBuilder(ABC):
    """
    Abstract base class for all builders.
    """

    def __init__(self, station: WeatherStation, records: pd.DataFrame, run_id: str):
        """
        Initialize the base processor with common attributes.

        Args:
            station (WeatherStation): The weather station metadata.
            records (pd.DataFrame): DataFrame of raw weather records.
            run_id (str): Unique identifier for this processing run.
        """
        self.station = station
        self.records = records
        self.run_id = run_id

    @abstractmethod
    def run(self, dry_run: bool) -> DailyRecord | MonthlyRecord:
        """
        Process the records and save the resulting DailyRecord or MonthlyRecord.

        Args:
            dry_run (bool): If True, don't save to database; if False, save record.

        Returns:
            DailyRecord | MonthlyRecord: The processed record if successful, None otherwise.
        """

    @abstractmethod
    def _generate_record(self) -> DailyRecord | MonthlyRecord:
        """
        Process the records and return a DailyRecord or MonthlyRecord summary.

        Returns:
            DailyRecord | MonthlyRecord: The processed summary record.
        """

    @abstractmethod
    def _save_record(self, record: DailyRecord | MonthlyRecord) -> None:
        """
        Save the DailyRecord or MonthlyRecord to the database.

        Args:
            record (DailyRecord | MonthlyRecord): The record to save.
        """
