import datetime
import os
import concurrent.futures
import threading
import json
import pandas as pd
import uuid

from schema import WeatherRecord, DailyRecord
from database import save_daily_record, get_weather_records_for_station_and_date

print_red = lambda text: print(f"\033[91m{text}\033[00m")
print_green = lambda text: print(f"\033[92m{text}\033[00m")
print_yellow = lambda text: print(f"\033[93m{text}\033[00m")

class DailyProcessor:
    def __init__(self, station_set: list, date: datetime.date, single_thread: bool = False, dry_run: bool = True):
        self.station_set = list(set(station_set))
        self.single_thread = single_thread
        self.date = date

        self.max_threads = int(os.getenv("MAX_THREADS", 4))
        self.dry_run = dry_run

    def run(self):
        self.run_id = uuid.uuid4().hex

        print_yellow(f"Processing daily data for date {self.date} with run ID {self.run_id}")

        if len(self.station_set) == 0:
            print_red("No active stations found!")
            return
        
        if self.single_thread:
            self.single_thread_processing(self.station_set)
        else:
            self.multithread_processing(self.station_set)

        print_green(f"Processing complete for run ID {self.run_id} for date {self.date}")
        
    def single_thread_processing(self, stations):
        for station in stations:
            self.process_station(station)

    def multithread_processing(self, stations):
        def process_chunk(chunk, chunk_number):
            print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
            for station in chunk:
                self.process_station(station)

        chunk_size = len(stations) // self.max_threads
        remainder_size = len(stations) % self.max_threads
        chunks = []
        for i in range(self.max_threads):
            start = i * chunk_size
            end = start + chunk_size
            chunks.append(stations[start:end])
        for i in range(remainder_size):
            chunks[i].append(stations[-(i+1)])
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for i, chunk in enumerate(chunks):
                executor.submit(process_chunk, chunk, chunk_number=i)

    def process_station(self, station_id: str): # station is a tuple like id, location
        print_yellow(f"Processing station {station_id}")
        
        try:
            records = get_weather_records_for_station_and_date(station_id, self.date) # tuples
            records = [WeatherRecord(*record) for record in records] # list of WeatherRecord objects
            
            if len(records) == 0 or records is None:
                print(f"No weather records retrieved for station {station_id}")
                return
            
            daily_record = build_daily_record(records, self.date)
            daily_record.cook_run_id = self.run_id

            if not self.dry_run:
                save_daily_record(daily_record)
                print_green(f"Daily record saved for station {station_id}")
            else:
                print(json.dumps(daily_record.__dict__, indent=4, sort_keys=True, default=str))
                print_green(f"Dry run enabled, record not saved for station {station_id}")

        except Exception as e:
            print_red(f"Error processing station {station_id}: {e}")

        print()


def calculate_flagged(df: pd.DataFrame) -> bool:
    return bool(df['flagged'].any())

def calculate_pressure(df: pd.DataFrame) -> tuple:
    if not df['pressure'].isnull().all():
        high_pressure = float(df['pressure'].max())
        low_pressure = float(df['pressure'].min())
    else:
        high_pressure = None
        low_pressure = None
    return high_pressure, low_pressure

def calculate_wind(df: pd.DataFrame) -> tuple:
    max_wind_speed = df[['wind_speed']].max().max()
    max_wind_gust = df[['max_wind_gust']].max().max()
    max_max_wind_speed = df[['max_wind_speed']].max().max()

    wind_columns = ['wind_speed', 'max_wind_speed', 'max_wind_gust']
    max_global_wind_speed = df[wind_columns].max().max()

    using_column = None
    if max_wind_speed == max_global_wind_speed:
        using_column = 'wind_speed'
    elif max_max_wind_speed == max_global_wind_speed:
        using_column = 'max_wind_speed'
    elif max_wind_gust == max_global_wind_speed:
        using_column = 'max_wind_gust'

    if pd.isna(max_global_wind_speed):
        high_wind_speed = None
        high_wind_direction = None
    else:
        high_wind_speed = float(max_global_wind_speed)
        high_wind_direction = df.loc[df[using_column].idxmax()]['wind_direction']
        if pd.isna(high_wind_direction):
            high_wind_direction = None
        else:
            high_wind_direction = float(high_wind_direction)

    return high_wind_speed, high_wind_direction

def calculate_temperature(df: pd.DataFrame) -> tuple:
    high_temperature = float(df[['temperature', 'max_temp']].max().max())
    low_temperature = float(df[['temperature', 'min_temp']].min().min())
    avg_temperature = float(df['temperature'].mean())

    return high_temperature, low_temperature, avg_temperature

def calculate_rain(df: pd.DataFrame) -> float:
    max_cum_rain = float(df['cumulative_rain'].max())
    if max_cum_rain == 0 or pd.isna(max_cum_rain):
        total_rain = float(df['rain'].sum())
    else:
        total_rain = max_cum_rain
    return total_rain

def calculate_humidity(df: pd.DataFrame) -> tuple:
    high_humidity = float(df['humidity'].max())
    low_humidity = float(df['humidity'].min())
    avg_humidity = float(df['humidity'].mean())
    return high_humidity, low_humidity, avg_humidity

def build_daily_record(records: list[WeatherRecord], date: datetime.datetime) -> DailyRecord:
    df = pd.DataFrame([{
        'id': record.id,
        'station_id': record.station_id,
        'source_timestamp': record.source_timestamp,
        'temperature': record.temperature,
        'wind_speed': record.wind_speed,
        'max_wind_speed': record.max_wind_speed,
        'wind_direction': record.wind_direction,
        'rain': record.rain,
        'cumulative_rain': record.cumulative_rain,
        'humidity': record.humidity,
        'pressure': record.pressure,
        'flagged': record.flagged,
        'taken_timestamp': record.taken_timestamp,
        'max_temp': record.max_temp,
        'min_temp': record.min_temp,
        'max_wind_gust': record.max_wind_gust
    } for record in records])

    flagged = calculate_flagged(df)
    high_pressure, low_pressure = calculate_pressure(df)
    high_wind_speed, high_wind_direction = calculate_wind(df)
    high_temperature, low_temperature, avg_temperature = calculate_temperature(df)
    high_humidity, low_humidity, avg_humidity = calculate_humidity(df)
    total_rain = calculate_rain(df)

    return DailyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=date,
        high_temperature=high_temperature,
        low_temperature=low_temperature,
        high_wind_gust=high_wind_speed,
        high_wind_direction=high_wind_direction,
        high_pressure=high_pressure,
        low_pressure=low_pressure,
        rain=total_rain,
        flagged=flagged,
        finished=True,
        cook_run_id=None,
        avg_temperature=avg_temperature,
        high_humidity=high_humidity,
        avg_humidity=avg_humidity,
        low_humidity=low_humidity
    )