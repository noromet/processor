"""MonthlyUpdateQueue Schema"""

import uuid
from dataclasses import dataclass


@dataclass
class MonthlyUpdateQueue:
    """
    Represents an entry in the monthly update queue.

    Attributes:
        id (uuid.UUID): Unique identifier for the monthly update queue entry.
        station_id (uuid.UUID): Identifier for the weather station associated with this entry.
        year (int): The year of the monthly record to be processed.
        month (int): The month of the monthly record to be processed (1-12).
    """

    id: uuid.UUID
    station_id: uuid.UUID
    year: int
    month: int
