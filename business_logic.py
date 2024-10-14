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
        gathererRunId=tuple[11]
    )

def build_daily_record(records: list[WeatherRecord], date: datetime.datetime) -> DailyRecord:
    # Convert records to DataFrame
    df = pd.DataFrame([{
        'station_id': record.station_id,
        'taken_timestamp': record.taken_timestamp,
        'wind_speed': record.wind_speed,
        'max_wind_speed': record.max_wind_speed,
        'wind_direction': record.wind_direction,
        'temperature': record.temperature,
        'pressure': record.pressure,
        'rain': record.rain,
        'flagged': record.flagged
    } for record in records])

    # Calculate summary statistics
    high_temperature = float(df['temperature'].max())
    low_temperature = float(df['temperature'].min())
    high_pressure = float(df['pressure'].max())
    low_pressure = float(df['pressure'].min())
    total_rain = float(df['rain'].sum())
    flagged = bool(df['flagged'].any())

    #wind
    are_all_max_wind_speed_na = df['max_wind_speed'].isna().all()
    if are_all_max_wind_speed_na:
        high_wind_speed = float(df['wind_speed'].max())
        high_wind_direction = float(df.loc[df['wind_speed'].idxmax()]['wind_direction'])
    else:
        high_wind_speed = float(df['max_wind_speed'].max())
        high_wind_direction = float(df.loc[df['max_wind_speed'].idxmax()]['wind_direction'])

    # Handle cases where no valid data was found
    high_wind_speed = high_wind_speed if pd.notna(high_wind_speed) else 0.0
    high_wind_direction = high_wind_direction if pd.notna(high_wind_direction) else 0.0
    high_temperature = high_temperature if pd.notna(high_temperature) else 0.0
    low_temperature = low_temperature if pd.notna(low_temperature) else 0.0
    high_pressure = high_pressure if pd.notna(high_pressure) else 0.0
    low_pressure = low_pressure if pd.notna(low_pressure) else 0.0
    total_rain = total_rain if pd.notna(total_rain) else 0.0

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