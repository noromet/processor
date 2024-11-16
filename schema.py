import datetime
import uuid

class WeatherStation:
    def __init__(self, id: uuid.uuid4, token: str):
        self.id = id
        self.token = token
        
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
                 gathererRunId: uuid.uuid4, 
                 cumulativeRain: float,
                 maxTemp: float,
                 minTemp: float,):
        
        self.id = id
        self.station_id = station_id
        self.source_timestamp = source_timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.max_wind_speed = max_wind_speed
        self.wind_direction = wind_direction
        self.rain = rain
        self.cumulativeRain = cumulativeRain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged
        self.taken_timestamp = datetime.datetime.now()
        self.gathererRunId = gathererRunId
        self.maxTemp = maxTemp
        self.minTemp = minTemp


class DailyRecord:
    def __init__(self, id: uuid.uuid4, station_id: uuid.uuid4, date: datetime.date, high_temperature: float = None, low_temperature: float = None, high_wind_speed: float = None, high_wind_direction: float = None, high_pressure: float = None, low_pressure: float = None, rain: float = None, flagged: bool = False, finished: bool = False, cookRunId: uuid.uuid4 = None):
        self.id = id
        self.station_id = station_id
        self.date = date
        self.high_temperature = high_temperature
        self.low_temperature = low_temperature
        self.high_wind_speed = high_wind_speed
        self.high_wind_direction = high_wind_direction
        self.high_pressure = high_pressure
        self.low_pressure = low_pressure
        self.rain = rain
        self.flagged = flagged
        self.finished = finished
        self.cookRunId = cookRunId