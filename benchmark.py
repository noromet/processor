"""
Benchmarking script for the Processor library.
"""

import datetime
from unittest.mock import patch
import time
import logging

import matplotlib.pyplot as plt
import numpy as np
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

    results = {}
    thread_counts = [1]  # Default to single thread benchmark

    for thread_count in thread_counts:
        results[thread_count] = {}
        for n_stations in N_STATIONS:
            print(
                f"Running benchmark with {n_stations} stations and {thread_count} threads"
            )
            elapsed_time = _run(n_stations)
            results[thread_count][n_stations] = elapsed_time
            print(f"Elapsed time: {elapsed_time:.2f} seconds")

    # Plot the results with enhanced styling
    plot_results(results)


def plot_results(results):
    """
    Plot benchmark results with enhanced styling.

    Args:
        results (dict): Dictionary of thread counts to dictionaries
            of station counts to elapsed times.
    """
    # Get max elapsed time for setting up the plot range
    max_elapsed_time = (
        max(max(times.values()) for times in results.values()) * 1.1
    )  # Add 10% for margin

    # Plot with improvements
    plt.figure(figsize=(12, 8))
    colors = ["#52828b", "#578651", "#b6af4d"]
    markers = ["o", "s", "^"]
    line_styles = ["-", "-", "-"]

    # Set specific y-ticks with a reasonable range - every 2 seconds
    y_ticks = np.arange(0, max_elapsed_time, 2)
    plt.yticks(y_ticks)

    # Add horizontal grid lines at y-tick positions first (so they appear below the data lines)
    # Skip the gridline at y=0
    for y in y_ticks:
        if y > 0:  # Skip the gridline at y=0
            plt.axhline(y=y, color="gray", linestyle=":", alpha=0.5)

    # Add vertical lines at specified station counts (below the data lines)
    # Skip the gridline at x=0
    for station in N_STATIONS:
        if (
            station > 0
        ):  # This check is redundant as N_STATIONS doesn't include 0, but included for clarity
            plt.axvline(x=station, color="gray", linestyle=":", alpha=0.7)

    # Plot the data lines last so they appear on top of the grid
    for i, thread_count in enumerate(sorted(results.keys())):
        x = list(results[thread_count].keys())
        y = list(results[thread_count].values())
        plt.plot(
            x,
            y,
            label=f"{thread_count} threads",
            color=colors[i % len(colors)],
            marker=markers[i % len(markers)],
            linewidth=2,
            linestyle=line_styles[i % len(line_styles)],
            markersize=8,
            zorder=10,
        )  # Added zorder to ensure lines are on top

        # Add data point labels
        for _, (station_count, elapsed_time) in enumerate(zip(x, y)):
            plt.annotate(
                f"{elapsed_time:.1f}s",
                (station_count, elapsed_time),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
                zorder=11,
            )  # Ensure labels are also on top

    # Configure the plot
    plt.xlabel("Número de estaciones", fontsize=12)
    plt.ylabel("Tiempo (s)", fontsize=12)
    plt.title("Benchmark: Tiempo de ejecución vs Número de Estaciones", fontsize=14)

    # Set specific x-ticks at station values with 0 included
    plt.xticks([0] + N_STATIONS)

    # Set axes to start at 0,0
    plt.xlim(0, max(N_STATIONS) * 1.05)  # Add a small margin on the right
    plt.ylim(0, max_elapsed_time * 1.05)  # Add a small margin on the top

    plt.legend(fontsize=10, loc="upper left")
    plt.grid(False)  # Disable default grid since we're adding custom grid lines
    plt.tight_layout()

    # Optional: Add some more polish
    plt.gca().spines["top"].set_visible(False)
    plt.gca().spines["right"].set_visible(False)

    # Save the plot
    plt.savefig("benchmark_results.png", dpi=300, bbox_inches="tight")
    print("Plot saved as 'benchmark_results.png'")

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
