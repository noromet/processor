"""ProcessorThread Schema"""

import uuid
import datetime
from dataclasses import dataclass


@dataclass
class ProcessorThread:
    """
    Represents a thread that processes weather data.

    Attributes:
        thread_id (uuid.UUID): Unique identifier for the processing thread.
        thread_timestamp (datetime.datetime): Timestamp when the thread was created.
        command (str): Console command that launched the processing thread.
        processed_date (datetime.date): Date when the data was processed.
    """

    thread_id: uuid.UUID
    thread_timestamp: datetime.datetime
    command: str
    processed_date: datetime.date
