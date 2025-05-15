#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command-line utility to download historical weather data from JMA obsdl.
Data is fetched month by month and saved as CSV files.
This script acts as the main entry point and orchestrator for JMA data retrieval.
"""

import argparse
import datetime
import logging
import os
import time
import calendar
from typing import List, Tuple, Dict, Any, Optional, Iterator
import sys 

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import requests 
from bs4 import BeautifulSoup 
# Pandas is used by the handler, not directly here for main data manipulation
# import pandas as pd 

from data_sources.jma.db_importer import JMAWeatherDBImporter 
from db.duckdb_connection import DuckDBConnection 

# Import from new local modules
from data_sources.jma.jma_config import (
    JMA_STATIONS, 
    DEFAULT_ELEMENT_CODES, 
    JMA_INDEX_URL,
    DEFAULT_REQUEST_RATE_SECONDS
    # MAX_RETRIES, INITIAL_RETRY_DELAY_SECONDS, JMA_POST_URL are used by handler
)
from data_sources.jma.jma_data_handler import (
    build_jma_payload,
    fetch_jma_csv_data,
    parse_jma_csv
)

# Configure logging (remains the same)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants moved to jma_config.py are removed from here:
# JMA_BASE_URL, JMA_INDEX_URL, JMA_POST_URL, MAX_RETRIES, INITIAL_RETRY_DELAY_SECONDS, WIND_DIRECTION_TO_DEGREES

# calculate_wind_components moved to jma_data_handler.py

def parse_cli_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download JMA historical weather data (obsdl) month by month via Command Line.",
        epilog=f"Example: python {os.path.basename(__file__)} --stations 47662 --elements {','.join(DEFAULT_ELEMENT_CODES)} --start 2023-01-01 --end 2023-02-28 --outdir ./jma_data_csv"
    )
    parser.add_argument(
        "--stations",
        type=str,
        required=False, 
        default=None,   
        help="Comma-separated 5-digit station codes (e.g., 47662,47626). If not provided, uses stations from jma_config.py.",
    )
    parser.add_argument(
        "--elements",
        type=str,
        default=",".join(DEFAULT_ELEMENT_CODES), # Use default from config
        help=f"Comma-separated obsdl element codes (default: {','.join(DEFAULT_ELEMENT_CODES)}).",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date for data download (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date for data download (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--outdir", # This argument might become obsolete if CSVs are not saved locally anymore by this script
        type=str,
        default="./data", # Consider changing if local CSV saving is removed/optional
        help="Directory to save output CSV files (default: ./data). (Note: primary output is DB)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_REQUEST_RATE_SECONDS, # Use from config
        help=f"Seconds to wait between consecutive requests (default: {DEFAULT_REQUEST_RATE_SECONDS}).",
    )
    parser.add_argument("--db-path", default=None, help="Path to the DuckDB database file. If not provided, uses DB_PATH from .env or a default.")
    parser.add_argument("--skip-db-import", action='store_true', help="Skip importing data into the database.")
    args = parser.parse_args()

    # Validate and convert arguments
    try:
        args.start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
        args.end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        parser.error("Dates must be in YYYY-MM-DD format.")

    if args.end_date < args.start_date:
        parser.error("End date must be after or the same as start date.")

    if args.stations:
        args.station_list = [s.strip() for s in args.stations.split(",")]
    else:
        args.station_list = [station["id"] for station in JMA_STATIONS]
        station_names = [station["name"] for station in JMA_STATIONS]
        logger.info(f"No stations provided via CLI. Using {len(args.station_list)} stations from jma_config.py: {station_names}")

    args.element_list = [e.strip() for e in args.elements.split(",")]
    args.interval = "hourly" # Currently hardcoded, could be an arg if other intervals are supported by parsing logic
    
    return args

def get_interactive_args() -> argparse.Namespace:
    """Gets arguments interactively from the user, with defaults."""
    print("Entering interactive mode for JMA historical data downloader.")
    print("Data will be fetched at an hourly interval.")
    print("Press Enter to use the default value shown in (parentheses).")

    default_station_ids_from_config = [station["id"] for station in JMA_STATIONS]
    default_station_names_from_config = [station["name"] for station in JMA_STATIONS]
    default_stations_str_display = ", ".join([f"{name}({sid})" for sid, name in zip(default_station_ids_from_config, default_station_names_from_config)])
    default_stations_input_str = ",".join(default_station_ids_from_config)

    default_elements_str = ",".join(DEFAULT_ELEMENT_CODES)
    
    today = datetime.date.today()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    default_start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
    default_end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")
    default_outdir_prompt = "(leave blank to skip saving CSVs, default: no CSV output)" 
    default_rate = DEFAULT_REQUEST_RATE_SECONDS # Use from config

    stations_str_in = input(f"Enter station codes (comma-separated) (default: {default_stations_str_display} from config): ")
    stations_str = stations_str_in.strip() if stations_str_in.strip() else default_stations_input_str
    
    station_list = []
    if stations_str:
        station_list = [s.strip() for s in stations_str.split(",")]
        valid_stations = all(s_id.isdigit() and len(s_id) == 5 for s_id in station_list)
        if not valid_stations:
            print("Invalid station code format. Reverting to defaults from config.")
            station_list = default_station_ids_from_config
    else:
        logger.warning("Station input was unexpectedly empty. Using config defaults.")
        station_list = default_station_ids_from_config

    # Simplified validation loop (already present and seems okay)
    while not station_list or not all(s.isdigit() and len(s) == 5 for s in station_list if s):
        print("Invalid station code format or empty list. Please use 5-digit numbers, comma-separated.")
        stations_str_in = input(f"Enter station codes (default: {default_stations_str_display} from config): ")
        stations_str_raw = stations_str_in.strip()
        if stations_str_raw:
            station_list = [s.strip() for s in stations_str_raw.split(",")]
        elif default_stations_input_str:
             print(f"Using default stations from config: {default_stations_str_display}")
             station_list = default_station_ids_from_config
        else:
            print("No stations provided and no default stations in config. Please provide station IDs.")
            station_list = [] 
        if not station_list: continue

    elements_str_in = input(f"Enter obsdl element codes (default: {default_elements_str}): ")
    elements_str = elements_str_in.strip() if elements_str_in.strip() else default_elements_str
    element_list = [e.strip() for e in elements_str.split(",")]

    start_date_str = input(f"Enter start date (YYYY-MM-DD) (default: {default_start_date_str}): ")
    start_date_str = start_date_str.strip() if start_date_str.strip() else default_start_date_str
    start_date = None
    while start_date is None:
        try:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid date format.")
            start_date_str = input(f"Enter start date (YYYY-MM-DD) (default: {default_start_date_str}): ")
            start_date_str = start_date_str.strip() if start_date_str.strip() else default_start_date_str
            
    end_date_str = input(f"Enter end date (YYYY-MM-DD) (default: {default_end_date_str}): ")
    end_date_str = end_date_str.strip() if end_date_str.strip() else default_end_date_str
    end_date = None
    while end_date is None:
        try:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date < start_date:
                print("End date must be after or the same as start date.")
                end_date = None 
                end_date_str = input(f"Enter end date (YYYY-MM-DD, after {start_date_str}) (default: {default_end_date_str}): ")
                end_date_str = end_date_str.strip() if end_date_str.strip() else default_end_date_str
        except ValueError:
            print("Invalid date format.")
            end_date_str = input(f"Enter end date (YYYY-MM-DD, after {start_date_str}) (default: {default_end_date_str}): ")
            end_date_str = end_date_str.strip() if end_date_str.strip() else default_end_date_str

    interval = "hourly" # Hardcoded, as before
            
    outdir_in = input(f"Output directory for CSV files {default_outdir_prompt}: ")
    outdir = outdir_in.strip() # If blank, will be empty string, effectively skipping CSV save
        
    rate_str = input(f"Seconds to wait between requests (default: {default_rate}): ")
    rate_str = rate_str.strip() if rate_str.strip() else str(default_rate)
    rate = default_rate
    try:
        rate_val = float(rate_str)
        if rate_val > 0: rate = rate_val
        else: print("Rate must be positive. Using default.")
    except ValueError:
        print("Invalid number for rate. Using default.")

    args_namespace = argparse.Namespace(
        stations=stations_str, # Keep original input string for reference if needed
        station_list=station_list,
        elements=elements_str, # Keep original input string
        element_list=element_list,
        start=start_date_str, # Keep original input string
        start_date=start_date,
        end=end_date_str, # Keep original input string
        end_date=end_date,
        interval=interval,
        outdir=outdir, 
        rate=rate,
        db_path=None, 
        skip_db_import=False # Default to import
    )
    
    skip_import_in = input(f"Skip database import? (yes/NO, default: NO): ")
    if skip_import_in.strip().lower() in ['yes', 'y']:
        args_namespace.skip_db_import = True

    return args_namespace

def month_range(
    start_date: datetime.date, end_date: datetime.date
) -> Iterator[Tuple[int, int, int, int]]:
    """
    Generates (year, month, first_day, last_day) tuples for each month
    within the given date range (inclusive).
    """
    current_year = start_date.year
    current_month = start_date.month
    while True:
        first_day_of_month = 1
        _, last_day_of_month = calendar.monthrange(current_year, current_month)
        iter_start_date = datetime.date(current_year, current_month, first_day_of_month)
        if iter_start_date > end_date: break
        actual_first_day = first_day_of_month
        actual_last_day = last_day_of_month
        if current_year == start_date.year and current_month == start_date.month:
            actual_first_day = start_date.day
        if current_year == end_date.year and current_month == end_date.month:
            actual_last_day = end_date.day
            yield (current_year, current_month, actual_first_day, actual_last_day)
            break 
        else:
            yield (current_year, current_month, actual_first_day, actual_last_day)
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
        if datetime.date(current_year, current_month, 1) > end_date and \
           not (current_year == end_date.year and current_month == end_date.month):
            break

# build_payload moved to jma_data_handler.py
# fetch_month_data moved to jma_data_handler.py (as fetch_jma_csv_data)

def main():
    """Main execution function orchestrating the JMA data download and processing."""
    args: Optional[argparse.Namespace] = None
    if len(sys.argv) > 1 and not (len(sys.argv) == 2 and sys.argv[1] == "menu"): # Avoid parsing for 'menu'
        # Check if running in a context (like from main.py menu) that shouldn't parse CLI args here
        # A more robust check might be needed if this script can be called in many ways
        is_direct_run = True
        try:
            # Heuristic: if a known CLI-only arg is present, assume direct run.
            # This is imperfect. A dedicated flag from caller is better.
            temp_parser = argparse.ArgumentParser()
            temp_parser.add_argument("--start") # A required arg for CLI
            temp_args, _ = temp_parser.parse_known_args()
            if not temp_args.start and len(sys.argv) > 1 : # if --start is not there but other args are, maybe not direct run
                 # if it's just 'python jma_historical.py' (len==1) or '... menu' (len==2), interactive is fine
                 # but if 'python jma_historical.py some_unknown_arg', it's ambiguous
                 if len(sys.argv) > 1: # If any args are present other than script name
                    # Potentially called from elsewhere not intending CLI parsing here
                    # logger.info("Ambiguous execution context. Consider calling with --start or no args for interactive.")
                    # Forcing interactive if not clearly a CLI call with --start
                    pass # Let it fall to interactive or be handled by specific caller
                 
        except SystemExit: # Raised by parser.error
             return # Exit if CLI parsing fails early

        # Proceed with CLI parsing if it seems like a direct CLI call
        if any(arg.startswith("--start") for arg in sys.argv):
            logger.info("Command-line arguments detected. Parsing CLI arguments.")
            args = parse_cli_args()
        else:
            logger.info("No --start argument detected, assuming interactive or called from menu. Entering interactive mode.")
            args = get_interactive_args()

    else: # len(sys.argv) <=1 or it's 'menu'
        logger.info("No specific command-line arguments for JMA script detected. Entering interactive mode.")
        args = get_interactive_args()


    if not args:
        logger.error("Argument parsing failed. Exiting.")
        return

    phpsessid: Optional[str] = None

    # Create output directory if it doesn't exist (primarily for HTML dump, less for CSVs now)
    if not os.path.exists(args.outdir):
        try:
            os.makedirs(args.outdir)
            logger.info(f"Created output directory: {args.outdir}")
        except OSError as e:
            logger.error(f"Failed to create output directory {args.outdir}: {e}. PHPSESSID HTML dump might fail.")
            # Decide if to exit or continue without outdir for HTML

    with requests.Session() as session:
        try:
            logger.info(f"Initializing session with GET to {JMA_INDEX_URL}...")
            headers = { # Standard User-Agent
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            session.headers.update(headers)
            init_response = session.get(JMA_INDEX_URL, timeout=30, allow_redirects=True)
            init_response.raise_for_status()
            
            logger.info(f"Initial GET to {JMA_INDEX_URL} status: {init_response.status_code}. URL: {init_response.url}")

            # Save HTML for inspection (useful for PHPSESSID issues)
            html_output_filename = os.path.join(args.outdir, "jma_index_response.html")
            try:
                with open(html_output_filename, "w", encoding="utf-8") as f_html:
                    f_html.write(init_response.text)
                logger.info(f"Saved initial GET response HTML to {html_output_filename}")
            except IOError as e:
                logger.warning(f"Could not save JMA index HTML to {html_output_filename}: {e}")


            soup = BeautifulSoup(init_response.text, 'lxml')
            sid_input = soup.find('input', {'id': 'sid'})
            if sid_input and sid_input.get('value'):
                phpsessid = sid_input.get('value')
                logger.info(f"Extracted PHPSESSID from HTML (#sid input): {phpsessid}")
            
            if not phpsessid: # Fallback to cookie if HTML parsing failed
                phpsessid_from_cookie = session.cookies.get('PHPSESSID')
                if phpsessid_from_cookie:
                    logger.info(f"Using PHPSESSID from session cookie: {phpsessid_from_cookie}")
                    phpsessid = phpsessid_from_cookie
                else:
                    logger.error("PHPSESSID NOT FOUND in HTML or cookies. Subsequent requests will likely fail.")
                    # Depending on strictness, could exit here.

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed initial GET to {JMA_INDEX_URL}: {e}. Exiting.")
            return
        
        # Ensure phpsessid is available before proceeding with data fetching loop
        if not phpsessid:
            logger.error("Cannot proceed without PHPSESSID. Exiting.")
            return

        all_station_data_dfs = [] # To potentially combine or save individual CSVs if needed later

        for station_code in args.station_list:
            logger.info(f"Processing station: {station_code}")
            for year, month, day_from, day_to in month_range(args.start_date, args.end_date):
                logger.info(
                    f"Fetching data for station {station_code}, Year: {year}, Month: {month:02d} (Days: {day_from}-{day_to}), Interval: {args.interval}"
                )
                
                payload = build_jma_payload(
                    station_code, 
                    args.element_list, 
                    args.interval, 
                    year, month, 
                    day_from, day_to,
                    phpsessid
                )
                
                csv_content = fetch_jma_csv_data(session, payload)

                if csv_content:
                    # Raw CSV logging (optional, can be verbose)
                    # logger.debug("Raw decoded CSV content (first 10 lines):")
                    # for i, line in enumerate(csv_content.splitlines()[:10]):
                    #     logger.debug(f"RAW_CSV_{i:02d}: {line}")

                    df = parse_jma_csv(csv_content, station_code, year, month, args.interval)

                    if df is not None and not df.empty:
                        logger.info(f"Successfully parsed data for station {station_code}, {year}-{month:02d}. Shape: {df.shape}")
                        all_station_data_dfs.append(df) # Collect for potential later use

                        # --- DB Import ---
                        if not args.skip_db_import:
                            logger.info(f"Attempting DB import for station {station_code}, {year}-{month:02d}...")
                            try:
                                with JMAWeatherDBImporter(db_path=args.db_path) as importer:
                                    inserted_count = importer.import_dataframe(df)
                                    logger.info(f"DB import successful for {station_code}, {year}-{month:02d}. Rows: {inserted_count}.")
                            except Exception as e_db:
                                logger.error(f"DB import FAILED for station {station_code}, {year}-{month:02d}: {e_db}", exc_info=True)
                        else:
                            logger.info(f"Skipping DB import for {station_code}, {year}-{month:02d} as per flag.")
                        
                        # --- Optional: Save individual CSV (if outdir is used) ---
                        if args.outdir:
                            try:
                                # Ensure month, day_from, day_to are defined in this scope for filename
                                # These come from the month_range loop: year, month, day_from, day_to
                                file_name = f"jma_weather_{station_code}_{year}{month:02d}_days{day_from:02d}-{day_to:02d}_{args.interval}.csv"
                                output_path = os.path.join(args.outdir, file_name)
                                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                                logger.info(f"Saved data to CSV: {output_path}")
                            except IOError as e_io:
                                logger.error(f"Failed to save CSV {output_path}: {e_io}")
                            except NameError as e_name: # Catch if year, month etc. are not in scope
                                logger.error(f"Failed to create filename for CSV due to missing variables: {e_name}")

                    elif df is None: # Explicit None from parser means critical error
                        logger.error(f"Parsing returned None for {station_code}, {year}-{month:02d}. Critical parsing error.")
                    else: # df is empty DataFrame
                        logger.warning(f"No data parsed (empty DataFrame) for station {station_code}, {year}-{month:02d}. Skipping DB import.")
                else:
                    logger.error(
                        f"Failed to fetch CSV data for station {station_code}, {year}-{month:02d} after retries."
                    )
                
                logger.info(f"Waiting for {args.rate} seconds before next request...")
                time.sleep(args.rate)
    
    logger.info("JMA historical data processing finished.")
    
    # Example: If you wanted to combine all DFs at the end (outside the scope of DB import per file)
    # if all_station_data_dfs:
    #     final_combined_df = pd.concat(all_station_data_dfs, ignore_index=True)
    #     logger.info(f"All JMA data collected. Total rows: {len(final_combined_df)}. Shape: {final_combined_df.shape}")
        # final_combined_df.to_csv(os.path.join(args.outdir, "jma_all_combined_data.csv"), index=False)


if __name__ == "__main__":
    main() 