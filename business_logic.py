from schema import WeatherRecord, DailyRecord
import uuid
import pandas as pd
import datetime

def construct_record(tuple) -> WeatherRecord:
    return WeatherRecord(
        id=tuple[0],
        station_id=tuple[1],
        source_timestamp=tuple[2],
        temperature=tuple[3],
        wind_speed=tuple[4],
        max_wind_speed=tuple[5],
        wind_direction=tuple[6],
        rain=tuple[7],
        humidity=tuple[8],
        pressure=tuple[9],
        flagged=tuple[10],
        gathererRunId=tuple[11],
        cumulativeRain=tuple[12],
        maxTemp=tuple[13],
        minTemp=tuple[14],
        maxWindGust=tuple[15]
    )

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
    max_wind_gust = df[['maxWindGust']].max().max()
    max_max_wind_speed = df[['max_wind_speed']].max().max()

    wind_columns = ['wind_speed', 'max_wind_speed', 'maxWindGust']
    max_global_wind_speed = df[wind_columns].max().max()

    using_column = None
    if max_wind_speed == max_global_wind_speed:
        using_column = 'wind_speed'
    elif max_max_wind_speed == max_global_wind_speed:
        using_column = 'max_wind_speed'
    elif max_wind_gust == max_global_wind_speed:
        using_column = 'maxWindGust'

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
    max_temperature = df[['temperature', 'maxTemp']].max().max()
    high_temperature = float(max_temperature)

    min_temperature = df[['temperature', 'minTemp']].min().min()
    low_temperature = float(min_temperature)

    return high_temperature, low_temperature

def calculate_rain(df: pd.DataFrame) -> float:
    max_cum_rain = float(df['cumulativeRain'].max())
    if max_cum_rain == 0 or pd.isna(max_cum_rain):
        total_rain = float(df['rain'].sum())
    else:
        total_rain = max_cum_rain
    return total_rain

def build_daily_record(records: list[WeatherRecord], date: datetime.datetime) -> DailyRecord:
    df = pd.DataFrame([{
        'station_id': record.station_id,
        'taken_timestamp': record.taken_timestamp,
        'wind_speed': record.wind_speed,
        'max_wind_speed': record.max_wind_speed,
        'wind_direction': record.wind_direction,
        'temperature': record.temperature,
        'pressure': record.pressure,
        'rain': record.rain,
        'cumulativeRain': record.cumulativeRain,
        'flagged': record.flagged,
        'maxTemp': record.maxTemp,
        'minTemp': record.minTemp,
        'maxWindGust': record.maxWindGust
    } for record in records])

    flagged = calculate_flagged(df)
    high_pressure, low_pressure = calculate_pressure(df)
    high_wind_speed, high_wind_direction = calculate_wind(df)
    high_temperature, low_temperature = calculate_temperature(df)
    total_rain = calculate_rain(df)

    return DailyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=date,
        high_temperature=high_temperature,
        low_temperature=low_temperature,
        high_wind_speed=high_wind_speed,
        high_wind_direction=high_wind_direction,
        high_pressure=high_pressure,
        low_pressure=low_pressure,
        rain=total_rain,
        flagged=flagged,
        finished=True,
        cookRunId=None
    )