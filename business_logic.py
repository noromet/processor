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
        windGust=tuple[15]
    )

def build_daily_record(records: list[WeatherRecord], date: datetime.datetime) -> DailyRecord:
    # Convert records to DataFrame
    df = pd.DataFrame([{
        'station_id': record.station_id,
        'taken_timestamp': record.taken_timestamp,
        'wind_speed': record.wind_speed,
        'max_wind_speed': record.max_wind_speed, # nullable
        'wind_direction': record.wind_direction,
        'temperature': record.temperature,
        'pressure': record.pressure,
        'rain': record.rain,
        'cumulativeRain': record.cumulativeRain,
        'flagged': record.flagged,
        'maxTemp': record.maxTemp, # nullable
        'minTemp': record.minTemp, # nullable
        'windGust': record.windGust
    } for record in records])

    # Calculate summary statistics

    #flagged
    flagged = bool(df['flagged'].any())

    #pressure
    if not df['pressure'].isnull().all():
        high_pressure = float(df['pressure'].max())
        low_pressure = float(df['pressure'].min())
    else:
        high_pressure = None
        low_pressure = None

    #wind
    wind_columns = ['wind_speed', 'max_wind_speed', 'windGust']
    max_wind_speed = df[wind_columns].max().max()

    if pd.isna(max_wind_speed):
        high_wind_speed = None
        high_wind_direction = None
    else:
        high_wind_speed = float(max_wind_speed)
        
        #if max_wind_speed does not correspond to a value in wind_speed, we ned to leave high_wind_speed to null
        if df['wind_speed'].max() == max_wind_speed:
            high_wind_direction = float(df.loc[df['wind_speed'].idxmax()]['wind_direction'])
        else:
            high_wind_direction = None

    #temperature
    max_temperature = df[['temperature', 'maxTemp']].max().max()
    high_temperature = float(max_temperature)

    # Get the minimum temperature
    min_temperature = df[['temperature', 'minTemp']].min().min()
    low_temperature = float(min_temperature)

    #rain
    max_cum_rain = float(df['cumulativeRain'].max())
    if max_cum_rain == 0 or pd.isna(max_cum_rain):
        total_rain = float(df['rain'].sum())
    else:
        total_rain = max_cum_rain

    # Handle cases where no valid data was found
    high_wind_speed = high_wind_speed if pd.notna(high_wind_speed) else None
    high_wind_direction = high_wind_direction if pd.notna(high_wind_direction) else None
    high_temperature = high_temperature if pd.notna(high_temperature) else None
    low_temperature = low_temperature if pd.notna(low_temperature) else None
    high_pressure = high_pressure if pd.notna(high_pressure) else None
    low_pressure = low_pressure if pd.notna(low_pressure) else None
    total_rain = total_rain if pd.notna(total_rain) else None

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