"""WeatherStation Schema"""

import uuid
import zoneinfo
from dataclasses import dataclass


@dataclass
class WeatherStation:
    """
    Represents a weather station.

    Attributes:
        id (uuid.UUID): Unique identifier for the weather station.
        location (str): Location of the weather station.
        local_timezone (zoneinfo.ZoneInfo): Timezone of the weather station.
    """

    id: uuid.UUID
    location: str
    local_timezone: zoneinfo.ZoneInfo
