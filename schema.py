import datetime
import uuid
        
class WeatherRecord:
    def __init__(self, 
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
                 max_temp: float,
                 min_temp: float,
                 max_wind_gust: float):
        
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
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.max_wind_gust = max_wind_gust


class DailyRecord:
    def __init__(self, 
                 id: uuid.uuid4, 
                 station_id: uuid.uuid4, 
                 date: datetime.date, 
                 high_temperature: float, 
                 low_temperature: float, 
                 high_wind_gust: float, 
                 high_wind_direction: float, 
                 high_pressure: float, 
                 low_pressure: float, 
                 rain: float, 
                 flagged: bool, 
                 finished: bool, 
                 cook_run_id: uuid.uuid4, 
                 avg_temperature: float, 
                 high_humidity: float, 
                 avg_humidity: float, 
                 low_humidity: float):
        
        self.id = id
        self.station_id = station_id
        self.date = date
        self.high_temperature = high_temperature
        self.low_temperature = low_temperature
        self.high_wind_gust = high_wind_gust
        self.high_wind_direction = high_wind_direction
        self.high_pressure = high_pressure
        self.low_pressure = low_pressure
        self.rain = rain
        self.flagged = flagged
        self.finished = finished
        self.cook_run_id = cook_run_id
        self.avg_temperature = avg_temperature
        self.high_humidity = high_humidity
        self.avg_humidity = avg_humidity
        self.low_humidity = low_humidity


class MonthlyRecord:
    def __init__(self, 
                    id: uuid.uuid4, 
                    station_id: uuid.uuid4, 
                    date: datetime.datetime, 
                    avg_high_temperature: float, 
                    avg_low_temperature: float, 
                    avg_avg_temperature: float, 
                    avg_humidity: float, 
                    avg_max_wind_gust: float, 
                    avg_pressure: float, 
                    high_high_temperature: float, 
                    low_low_temperature: float, 
                    high_high_humidity: float, 
                    low_low_humidity: float, 
                    high_max_wind_gust: float, 
                    high_high_pressure: float, 
                    low_low_pressure: float, 
                    cumulative_rainfall: float, 
                    cook_run_id: uuid.uuid4,
                    finished: bool):
        self.id = id
        self.station_id = station_id
        self.date = date
        self.avg_high_temperature = avg_high_temperature
        self.avg_low_temperature = avg_low_temperature
        self.avg_avg_temperature = avg_avg_temperature
        self.avg_humidity = avg_humidity
        self.avg_max_wind_gust = avg_max_wind_gust
        self.avg_pressure = avg_pressure
        self.high_high_temperature = high_high_temperature
        self.low_low_temperature = low_low_temperature
        self.high_high_humidity = high_high_humidity
        self.low_low_humidity = low_low_humidity
        self.high_max_wind_gust = high_max_wind_gust
        self.high_high_pressure = high_high_pressure
        self.low_low_pressure = low_low_pressure
        self.cumulative_rainfall = cumulative_rainfall
        self.cook_run_id = cook_run_id
        self.finished = finished