"""
Benchmarking script for the Processor library.
"""

import datetime
from unittest.mock import patch
import time
import logging

import matplotlib.pyplot as plt
import pandas as pd

from processor import Processor
from processor.builders import DailyBuilder
from processor.database import Database
from processor.schema import WeatherStation, WeatherRecord

logging.disable(logging.CRITICAL)  # Disable logging for test runs

N_STATIONS = [10, 100, 250, 500, 750, 1000]


def _get_records_for_day(station: WeatherStation) -> pd.DataFrame:
    """
    Generate mock records for a day for a specific weather station.

    Creates a set of mock weather records at 15-minute intervals for a 24-hour period.

    Args:
        station (WeatherStation): The weather station to generate records for.

    Returns:
        pd.DataFrame: DataFrame containing mock weather records.
    """

    n_records = 24 * (60 / 15)

    records = []
    for i in range(int(n_records)):
        record = WeatherRecord(
            id=f"record_{i}",
            station_id=station.id,
            source_timestamp=datetime.datetime.now(),
            temperature=20.0 + i * 0.1,
            wind_speed=5.0 + i * 0.1,
            max_wind_speed=10.0 + i * 0.1,
            wind_direction=180.0 + i * 0.1,
            rain=0.0 + i * 0.1,
            humidity=50.0 + i * 0.1,
            pressure=1013.0 + i * 0.1,
            flagged=False,
            taken_timestamp=datetime.datetime.now(),
            gatherer_thread_id="gatherer_1",
            cumulative_rain=0.0 + i * 0.1,
            max_temperature=25.0 + i * 0.1,
            min_temperature=15.0 + i * 0.1,
            wind_gust=7.0 + i * 0.1,
            max_wind_gust=12.0 + i * 0.1,
        )
        records.append(record)

    return pd.DataFrame(records)


def benchmark():
    """
    Entry point to the benchmark and graphing process.

    Runs performance tests with varying numbers of weather stations and
    generates a graph showing processing time vs. number of stations.
    The results are saved as a PNG image and displayed on screen.

    Returns:
        None
    """

    results = {n_stations: 0 for n_stations in N_STATIONS}

    for n_stations in N_STATIONS:
        print(f"Running benchmark with {n_stations} stations.")
        elapsed_time = _run(n_stations)
        results[n_stations] = elapsed_time
        print(f"Elapsed time: {elapsed_time:.2f} seconds")

    # Plot the results
    plt.figure(figsize=(12, 8))
    plt.plot(
        list(results.keys()),
        list(results.values()),
        marker="o",
        linestyle="-",
        color="blue",
    )
    plt.xlabel("Number of Stations")
    plt.ylabel("Processing Time (seconds)")
    plt.title("Processor Performance Benchmark")
    plt.grid(True)
    plt.savefig("benchmark_results.png")
    plt.show()


def _run(n_stations):
    """
    Run the benchmark for a given number of stations.

    Sets up mock weather stations and processors, populates the processing queue
    with DailyBuilder instances, and measures the time taken to process them.

    Args:
        n_stations (int): Number of stations to include in the benchmark.

    Returns:
        float: Elapsed time in seconds for processing all stations.
    """
    # Create stations list
    stations = [
        WeatherStation(
            id=f"station_{i}",
            location=f"Station {i}",
            local_timezone="Europe/Madrid",
        )
        for i in range(n_stations)
    ]

    # Create a mocked version of the processor
    with patch.object(
        Processor, "get_all_stations"
    ) as mock_get_all_stations, patch.object(
        Processor, "fill_up_daily_queue"
    ), patch.object(
        Database, "get_present_timezones"
    ) as get_present_timezones, patch.object(
        Database, "save_processor_thread"
    ):

        # Configure the mocks
        mock_get_all_stations.return_value = stations
        get_present_timezones.return_value = ["Europe/Madrid"]

        # Create processor instance
        processor = Processor(
            dry_run=True,
            process_date=datetime.date.today(),
            mode="daily",
            process_pending=False,
            all_stations=True,  # Changed to True to trigger get_all_stations
            station_id=None,
        )

        for i in range(n_stations):
            processor.processing_queue.put(
                DailyBuilder(
                    station=stations[i],
                    records=_get_records_for_day(stations[i]),
                    date=datetime.date.today(),
                    run_id=processor.run_id,
                )
            )

        # Time the execution
        start_time = time.monotonic()
        processor.run()  # Call the original run method
        end_time = time.monotonic()

        return end_time - start_time


if __name__ == "__main__":
    benchmark()
else:
    pass
