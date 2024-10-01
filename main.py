import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import json
import argparse
import datetime

from business_logic import build_daily_record, construct_record
from database import Database, get_all_stations, get_single_station, get_records_for_station_and_date

# region definitions
print_red = lambda text: print(f"\033[91m{text}\033[00m")
print_green = lambda text: print(f"\033[92m{text}\033[00m")
print_yellow = lambda text: print(f"\033[93m{text}\033[00m")
    
load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
DRY_RUN = False
# endregion

#region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Cook all stations")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("--date", type=datetime.date.fromisoformat, help="Date to process, in ISO format")
    parser.add_argument("--multithread-threshold", type=int, default=-1, help="Threshold for enabling multithreading")
    return parser.parse_args()

def validate_args(args):
    if args.all and args.id:
        raise ValueError("Cannot specify both --all and --id")
    
    if not args.all and not args.id:
        raise ValueError("Must specify --all or --id")
    
    if args.multithread_threshold == 0 or args.multithread_threshold < -1: #so, -1 or positive integer are valid
        raise ValueError("Invalid multithread threshold")
    
    #date needs to be validated: yesterday or before
    if args.date:
        if args.date > datetime.date.today():
            raise ValueError("Date must be yesterday's or earlier")
#endregion

# region processing
def process_station(station: tuple, date: datetime.date): # station is a tuple like id, name, location
    print_yellow(f"Processing station {station[0]} ({station[1]})")
    
    try:
        records = get_records_for_station_and_date(station[0], date) # tuples
        records = [construct_record(record) for record in records] # list of WeatherRecord objects
        
        if len(records) == 0 or records is None:
            print(f"No records retrieved for station {station[0]}")
            return
        
        daily_record = build_daily_record(records)

        if not DRY_RUN:
            Database.save_daily_record(daily_record)
            print_green(f"Daily record saved for station {station[0]}")
        else:
            print(json.dumps(daily_record.__dict__, indent=4, sort_keys=True, default=str))
            print_green(f"Dry run enabled, record not saved for station {station[0]}")

    except Exception as e:
        print_red(f"Error processing station {station[0]}: {e}")

    print()

def process_chunk(chunk, chunk_number):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
    for station in chunk:
        process_station(station)

def multithread_processing(stations):
    chunk_size = len(stations) // MAX_THREADS
    remainder_size = len(stations) % MAX_THREADS
    chunks = []
    for i in range(MAX_THREADS):
        start = i * chunk_size
        end = start + chunk_size
        chunks.append(stations[start:end])
    for i in range(remainder_size):
        chunks[i].append(stations[-(i+1)])
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for i, chunk in enumerate(chunks):
            executor.submit(process_chunk, chunk, chunk_number=i)

def process_all(multithread_threshold, date):
    stations = get_all_stations()

    if len(stations) == 0:
        print_red("No active stations found!")
        return

    print(f"Processing {len(stations)} stations")
    
    if multithread_threshold == -1 or len(stations) < multithread_threshold:
        for station in stations:
            process_station(station, date)

    elif len(stations) >= multithread_threshold:
        multithread_processing(stations)

    else:
        raise ValueError("Invalid multithread threshold")
    
def process_single(station_id, date):
    station = get_single_station(station_id)
    if station is None:
        print_red(f"Station {station_id} not found")
        return
    process_station(station, date)

# endregion

# region main
def main():
    Database.initialize(DB_URL)

    args = get_args()
    validate_args(args)

    global DRY_RUN
    DRY_RUN = args.dry_run
    multithread_threshold = args.multithread_threshold

    if args.dry_run:
        print_yellow("[Dry run enabled]")
    else:
        print_yellow("[Dry run disabled]")

    if args.id:
        process_single(args.id, args.date)

    else:
        process_all(multithread_threshold, args.date)
                
    Database.close_all_connections()
    
if __name__ == "__main__":
    main()

# endregion