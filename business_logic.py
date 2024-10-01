from schema import WeatherRecord, DailyRecord
import uuid

def construct_record(tuple) -> WeatherRecord:
    return WeatherRecord(
        id=tuple[0],
        station_id=tuple[1],
        source_timestamp=tuple[2],
        temperature=tuple[3],
        wind_speed=tuple[4],
        wind_direction=tuple[5],
        rain=tuple[6],
        humidity=tuple[7],
        pressure=tuple[8],
        flagged=tuple[9]
    )

def build_daily_record(records: list[WeatherRecord]) -> DailyRecord:
    print(f"Building daily record for station {records[0].station_id} on {records[0].taken_timestamp.date()}. {len(records)} records found.")

    high_wind_speed = float('-inf')
    high_wind_direction = None
    high_temperature = float('-inf')
    low_temperature = float('inf')
    high_pressure = float('-inf')
    low_pressure = float('inf')
    total_rain = 0
    flagged = False

    for record in records:
        if record.wind_speed is not None and record.wind_speed > high_wind_speed:
            high_wind_speed = record.wind_speed
            high_wind_direction = record.wind_direction

        if record.temperature is not None:
            if record.temperature > high_temperature:
                high_temperature = record.temperature
            if record.temperature < low_temperature:
                low_temperature = record.temperature

        if record.pressure is not None:
            if record.pressure > high_pressure:
                high_pressure = record.pressure
            if record.pressure < low_pressure:
                low_pressure = record.pressure

        if record.rain is not None:
            total_rain += record.rain

        if record.flagged:
            flagged = True

    # Handle cases where no valid data was found
    high_wind_speed = high_wind_speed if high_wind_speed != float('-inf') else 0
    high_temperature = high_temperature if high_temperature != float('-inf') else 0
    low_temperature = low_temperature if low_temperature != float('inf') else 0
    high_pressure = high_pressure if high_pressure != float('-inf') else 0
    low_pressure = low_pressure if low_pressure != float('inf') else 0

    return DailyRecord(
        id=str(uuid.uuid4()),
        station_id=records[0].station_id,
        date=records[0].taken_timestamp.date(),
        high_temperature=high_temperature,
        low_temperature=low_temperature,
        high_wind_speed=high_wind_speed,
        high_wind_direction=high_wind_direction,
        high_pressure=high_pressure,
        low_pressure=low_pressure,
        rain=total_rain,
        flagged=flagged,
        finished=True
    )