import datetime
import uuid
import zoneinfo

class WeatherRecord:
    def __init__(
        self,
        id: uuid.uuid4,
        station_id: uuid.uuid4,
        source_timestamp: datetime.datetime,
        temperature: float,
        wind_speed: float,
        max_wind_speed: float,
        wind_direction: float,
        rain: float,
        humidity: float,
        pressure: float,
        flagged: bool,
        taken_timestamp: datetime.datetime,
        gatherer_thread_id: uuid.uuid4,
        cumulative_rain: float,
        max_temperature: float,
        min_temperature: float,
        wind_gust: float,
        max_wind_gust: float
    ):

        self.id = id
        self.station_id = station_id
        self.source_timestamp = source_timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.max_wind_speed = max_wind_speed
        self.wind_direction = wind_direction
        self.rain = rain
        self.cumulative_rain = cumulative_rain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged
        self.taken_timestamp = taken_timestamp
        self.gatherer_thread_id = gatherer_thread_id
        self.max_temperature = max_temperature
        self.min_temperature = min_temperature
        self.wind_gust = wind_gust
        self.max_wind_gust = max_wind_gust


class DailyRecord:
    def __init__(
        self,
        id: uuid.uuid4,
        station_id: uuid.uuid4,
        date: datetime.date,
        max_temperature: float,
        min_temperature: float,
        max_wind_gust: float,
        max_wind_speed: float,
        avg_wind_direction: float,
        max_pressure: float,
        min_pressure: float,
        rain: float,
        flagged: bool,
        finished: bool,
        cook_run_id: uuid.uuid4,
        avg_temperature: float,
        max_humidity: float,
        avg_humidity: float,
        min_humidity: float,
        timezone: zoneinfo.ZoneInfo,
    ):
        self.id = id
        self.station_id = station_id
        self.date = date
        self.rain = rain
        self.flagged = flagged
        self.finished = finished
        self.cook_run_id = cook_run_id
        self.avg_temperature = avg_temperature
        self.max_temperature = max_temperature
        self.min_temperature = min_temperature
        self.max_wind_gust = max_wind_gust
        self.max_wind_speed = max_wind_speed
        self.avg_wind_direction = avg_wind_direction
        self.max_pressure = max_pressure
        self.min_pressure = min_pressure
        self.max_humidity = max_humidity
        self.avg_humidity = avg_humidity
        self.min_humidity = min_humidity
        self.timezone = timezone

class MonthlyRecord:
    def __init__(
        self,
        id: uuid.uuid4,
        station_id: uuid.uuid4,
        date: datetime.date,
        avg_max_temperature: float,
        avg_min_temperature: float,
        avg_avg_temperature: float,
        avg_humidity: float,
        avg_max_wind_gust: float,
        avg_pressure: float,
        max_max_temperature: float,
        min_min_temperature: float,
        max_max_humidity: float,
        min_min_humidity: float,
        max_max_pressure: float,
        max_max_wind_gust: float,
        min_min_pressure: float,
        cumulative_rainfall: float,
        cook_run_id: uuid.uuid4,
        finished: bool = True,
    ):
        self.id = id
        self.station_id = station_id
        self.date = date
        self.avg_max_temperature = avg_max_temperature
        self.avg_min_temperature = avg_min_temperature
        self.avg_avg_temperature = avg_avg_temperature
        self.avg_humidity = avg_humidity
        self.avg_max_wind_gust = avg_max_wind_gust
        self.avg_pressure = avg_pressure
        self.max_max_temperature = max_max_temperature
        self.min_min_temperature = min_min_temperature
        self.max_max_humidity = max_max_humidity
        self.min_min_humidity = min_min_humidity
        self.max_max_pressure = max_max_pressure
        self.min_min_pressure = min_min_pressure
        self.cumulative_rainfall = cumulative_rainfall
        self.cook_run_id = cook_run_id
        self.finished = finished
        self.max_max_wind_gust = max_max_wind_gust