#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command-line utility to download historical weather data from JMA obsdl.
Data is fetched month by month and saved as CSV files.
"""

import argparse
import datetime
import json
import logging
import os
import time
import calendar
from typing import List, Tuple, Dict, Any, Optional, Iterator
import sys # Added for checking sys.argv
import io # For StringIO
import math # Added for math.sin, math.cos, math.radians

# Add the project root to the Python path
# Corrected calculation: Go up 3 levels from the current file
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import requests # From requirements.txt
from bs4 import BeautifulSoup # Added for HTML parsing
import pandas as pd # Added for DataFrame manipulation
# from db.duckdb_connection import DuckDBConnection # Added for DB connection
from data_sources.jma.db_importer import JMAWeatherDBImporter # Import the JMA importer (absolute)
from db.duckdb_connection import DuckDBConnection # Import base DB connection (absolute)
from data_sources.jma.jma_config import JMA_STATIONS # Import JMA_STATIONS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
JMA_BASE_URL = "https://www.data.jma.go.jp/risk/obsdl/"
# Per user prompt: "requests.Session() で index.php を GET → PHPSESSID を確保。"
# While session object handles cookies, explicit GET is requested.
JMA_INDEX_URL = JMA_BASE_URL + "index.php"
JMA_POST_URL = JMA_BASE_URL + "show/table"  # CSV ダウンロードは拡張子なしエンドポイント
MAX_RETRIES = 3
INITIAL_RETRY_DELAY_SECONDS = 5

WIND_DIRECTION_TO_DEGREES = {
    "北": 0.0,
    "北北東": 22.5,
    "北東": 45.0,
    "東北東": 67.5,
    "東": 90.0,
    "東南東": 112.5,
    "南東": 135.0,
    "南南東": 157.5,
    "南": 180.0,
    "南南西": 202.5,
    "南西": 225.0,
    "西南西": 247.5,
    "西": 270.0,
    "西北西": 292.5,
    "北西": 315.0,
    "北北西": 337.5,
    "静穏": None,  # Calm
}

def calculate_wind_components(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates sin and cos components for wind direction.
    Assumes 'wind_direction' column exists with Japanese text.
    Adds 'wind_direction_sin' and 'wind_direction_cos' columns.
    """
    if 'wind_direction' not in df.columns:
        logger.warning("'wind_direction' column not found. Skipping sin/cos calculation.")
        df['wind_direction_sin'] = pd.NA
        df['wind_direction_cos'] = pd.NA
        return df

    def get_angle(direction_str: str) -> Optional[float]:
        return WIND_DIRECTION_TO_DEGREES.get(direction_str)

    df['wind_angle_deg'] = df['wind_direction'].apply(get_angle)
    
    df['wind_direction_sin'] = df['wind_angle_deg'].apply(
        lambda deg: math.sin(math.radians(deg)) if pd.notna(deg) else pd.NA
    )
    df['wind_direction_cos'] = df['wind_angle_deg'].apply(
        lambda deg: math.cos(math.radians(deg)) if pd.notna(deg) else pd.NA
    )
    
    # If wind_speed is 0 and direction was '静穏' (calm), sin/cos might be NA.
    # Depending on model requirements, one might fill these with 0.
    # For now, let's keep them as NA if angle was None.
    # If wind_speed is 0, '静穏' is expected. If direction is not '静穏' but speed is 0,
    # it's a bit ambiguous. The JMA data usually has '静穏' for 0 wind speed.
    if 'wind_speed' in df.columns:
        df.loc[df['wind_speed'] == 0, 'wind_direction_sin'] = df.loc[df['wind_speed'] == 0, 'wind_direction_sin'].fillna(0.0)
        df.loc[df['wind_speed'] == 0, 'wind_direction_cos'] = df.loc[df['wind_speed'] == 0, 'wind_direction_cos'].fillna(0.0)


    df.drop(columns=['wind_angle_deg'], inplace=True, errors='ignore')
    logger.info("Added 'wind_direction_sin' and 'wind_direction_cos' columns.")
    return df

def parse_cli_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download JMA historical weather data (obsdl) month by month via Command Line.",
        epilog="Example: python jma_historical.py --stations 47662 --elements 201,401,301,610,703,503 --start 2023-01-01 --end 2023-02-28 --outdir ./jma_data_csv"
    )
    parser.add_argument(
        "--stations",
        type=str,
        required=False, # Changed to False
        default=None,   # Default to None, will use JMA_STATIONS if not provided
        help="Comma-separated 5-digit station codes (e.g., 47662,47626). If not provided, uses stations from jma_config.py.",
    )
    parser.add_argument(
        "--elements",
        type=str,
        required=True,
        help="Comma-separated obsdl element codes (e.g., 201,401,301).",
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
        "--outdir",
        type=str,
        default="./data",
        help="Directory to save output CSV files (default: ./data).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.2,
        help="Seconds to wait between consecutive requests (default: 1.2).",
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
        logger.info(f"No stations provided via CLI. Using {len(args.station_list)} stations from jma_config.py: {[station['name'] for station in JMA_STATIONS]}")

    args.element_list = [e.strip() for e in args.elements.split(",")]
    args.interval = "hourly"
    
    return args

def get_interactive_args() -> argparse.Namespace:
    """Gets arguments interactively from the user, with defaults."""
    print("Entering interactive mode for JMA historical data downloader.")
    print("Data will be fetched at an hourly interval.")
    print("Press Enter to use the default value shown in (parentheses).")

    # Default values from jma_config.py for stations
    default_station_ids_from_config = [station["id"] for station in JMA_STATIONS]
    default_station_names_from_config = [station["name"] for station in JMA_STATIONS]
    default_stations_str_display = ", ".join([f"{name}({sid})" for sid, name in zip(default_station_ids_from_config, default_station_names_from_config)])
    default_stations_input_str = ",".join(default_station_ids_from_config)

    default_elements_str = "201,401,301,610,703,503" # Temp, Precip, SunDur, Wind, SnowDepth, SolarRad
    
    today = datetime.date.today()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    default_start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
    default_end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")
    default_outdir = "./jma_data_csv"
    default_rate = 1.2

    stations_str_in = input(f"Enter station codes (comma-separated, e.g., 47662,47626) (default: {default_stations_str_display} from config): ")
    stations_str = stations_str_in.strip() if stations_str_in.strip() else default_stations_input_str
    
    station_list = []
    if stations_str: # Check if string is not empty after strip or if default was used
        station_list = [s.strip() for s in stations_str.split(",")]
        valid_stations = True
        for s_id in station_list:
            if not (s_id.isdigit() and len(s_id) == 5):
                valid_stations = False
                break
        if not valid_stations:
            print("Invalid station code format detected. Please use 5-digit numbers, comma-separated.")
            # Fallback to config default or ask again, here we simplify to config default if primary input fails
            print(f"Reverting to default stations from config: {default_stations_str_display}")
            station_list = default_station_ids_from_config
    else: # Should not happen if default_stations_input_str is non-empty
        logger.warning("Station input was empty, which is unexpected. Using config defaults.")
        station_list = default_station_ids_from_config


    while not station_list or not all(s.isdigit() and len(s) == 5 for s in station_list if s):
        print("Invalid station code format or empty list. Please use 5-digit numbers, comma-separated.")
        stations_str_in = input(f"Enter station codes (default: {default_stations_str_display} from config): ")
        stations_str_raw = stations_str_in.strip()
        if stations_str_raw: # User provided input
            station_list = [s.strip() for s in stations_str_raw.split(",")]
        elif default_stations_input_str: # User hit enter, use config default
             print(f"Using default stations from config: {default_stations_str_display}")
             station_list = default_station_ids_from_config
        else: # User hit enter AND config default is empty (should not happen with current jma_config.py)
            print("No stations provided and no default stations in config. Please provide station IDs.")
            station_list = [] # Keep it empty to re-trigger loop or handle error later

        if not station_list: # If still empty after logic, ask again
             continue


    elements_str_in = input(f"Enter obsdl element codes (e.g., 201,401,301) (default: {default_elements_str}): ")
    elements_str = elements_str_in.strip() if elements_str_in.strip() else default_elements_str
    element_list = [e.strip() for e in elements_str.split(",")]

    start_date_str_in = input(f"Enter start date (YYYY-MM-DD) (default: {default_start_date_str}): ")
    start_date_str = start_date_str_in.strip() if start_date_str_in.strip() else default_start_date_str
    start_date = None
    while start_date is None:
        try:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
            start_date_str_in = input(f"Enter start date (YYYY-MM-DD) (default: {default_start_date_str}): ")
            start_date_str = start_date_str_in.strip() if start_date_str_in.strip() else default_start_date_str
            
    end_date_str_in = input(f"Enter end date (YYYY-MM-DD) (default: {default_end_date_str}): ")
    end_date_str = end_date_str_in.strip() if end_date_str_in.strip() else default_end_date_str
    end_date = None
    while end_date is None:
        try:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date < start_date:
                print("End date must be after or the same as start date.")
                end_date = None 
                end_date_str_in = input(f"Enter end date (YYYY-MM-DD, after {start_date_str}) (default: {default_end_date_str}): ")
                end_date_str = end_date_str_in.strip() if end_date_str_in.strip() else default_end_date_str
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
            end_date_str_in = input(f"Enter end date (YYYY-MM-DD, after {start_date_str}) (default: {default_end_date_str}): ")
            end_date_str = end_date_str_in.strip() if end_date_str_in.strip() else default_end_date_str

    interval = "hourly"
            
    outdir_in = input(f"Output directory for CSV files (default: {default_outdir}): ")
    outdir = outdir_in.strip() if outdir_in.strip() else default_outdir
        
    rate_str_in = input(f"Seconds to wait between requests (default: {default_rate}): ")
    rate_str = rate_str_in.strip() if rate_str_in.strip() else str(default_rate)
    rate = default_rate
    try:
        rate_val = float(rate_str)
        if rate_val > 0:
            rate = rate_val
        else:
            print("Rate must be a positive number. Using default.")
            rate = default_rate
    except ValueError:
        print("Invalid number for rate. Using default.")
        rate = default_rate

    args_namespace = argparse.Namespace(
        stations=stations_str,
        station_list=station_list,
        elements=elements_str,
        element_list=element_list,
        start=start_date_str,
        start_date=start_date,
        end=end_date_str,
        end_date=end_date,
        interval=interval,
        outdir=outdir, 
        rate=rate,
        # Add db_path and skip_db_import for interactive mode
        db_path=None, # .env または DuckDBConnection のデフォルト解決に任せる
        skip_db_import=False # Default is to import
    )
    
    # Ask about DB path - REMOVED (already removed in previous step)
    # db_path_in = input(f"Enter database path (default: {args_namespace.db_path}): ")
    # if db_path_in.strip():
    #     args_namespace.db_path = db_path_in.strip()

    # Ask about skipping DB import
    skip_import_in = input(f"Skip database import? (yes/NO): ")
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
        # iter_end_date = datetime.date(current_year, current_month, last_day_of_month) # Not directly used for loop condition

        if iter_start_date > end_date:
            break
        
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
        
        # Ensure the loop doesn't go past the end_date's month if it was yielded already
        if datetime.date(current_year, current_month, 1) > end_date and \
           not (current_year == end_date.year and current_month == end_date.month):
            break


def build_payload(
    station_code: str,
    element_codes: List[str],
    interval: str,
    year: int,
    month: int,
    day_from: int,
    day_to: int,
    phpsessid: Optional[str],
) -> Dict[str, Any]:
    """
    Builds the payload for the POST request to JMA, based on a known working script.
    """
    aggrg_period_map = {"monthly": 0, "daily": 1, "hourly": 9} # hourly is 9

    payload = {
        "stationNumList": json.dumps([f"s{station_code}"]),
        "elementNumList": json.dumps([[str(c), ""] for c in element_codes]),
        "ymdList": json.dumps([year, year, month, month, day_from, day_to]),

        "aggrgPeriod": aggrg_period_map[interval], # Integer (e.g., 9)
        "csvFlag": 1,                              # Integer
        "rmkFlag": 1,                              # Integer
        "disconnectFlag": 1,                       # Integer
        "ymdLiteral": 1,                           # Integer
        "youbiFlag": 0,                            # Integer
        "kijiFlag": 0,                             # Integer
        "jikantaiFlag": 0,                         # Integer
        "jikantaiList": json.dumps([1, 24]),       # JSON string '[1, 24]'
        "interAnnualFlag": 1,                      # Integer
        "optionNumList": [],                       # Empty Python list
        "downloadFlag": "true",                    # String "true"
        "huukouFlag": 0,                           # Integer
    }
    
    if phpsessid:
        payload["PHPSESSID"] = phpsessid
    else:
        logger.warning("PHPSESSID not available, sending payload without it.")
        
    logger.debug(f"Built payload: {payload}")
    return payload


def fetch_month_data(
    session: requests.Session,
    payload: Dict[str, Any],
    post_url: str,
    max_retries: int,
    initial_retry_delay: int,
) -> Optional[str]:
    """
    Fetches data for a single month with retries and exponential backoff.
    Returns decoded CSV content as a string or None on failure.
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} to POST to {post_url} with payload subset: "
                        f"station: {payload.get('stationNumList')}, period: {payload.get('aggrgPeriod')}, "
                        f"ymd: {payload.get('ymdList')}")
            # ★★★ 追加: 送信する全ペイロードをログに出力 ★★★
            logger.info(f"Full payload being sent: {json.dumps(payload, ensure_ascii=False)}")

            response = session.post(post_url, data=payload, timeout=60)

            # ★★★ 追加: レスポンスヘッダーをログに出力 ★★★
            logger.info(f"Response headers: {response.headers}")
            # ★★★ 追加: レスポンスの生の内容をログに出力 (Shift-JISでデコード試行前) ★★★
            # Content-TypeがCSV系でなければ、テキストとしてログ出力
            # また、CSVであってもデコード前の状態を確認するために、最初の数バイトをログ出力
            if "csv" in response.headers.get("Content-Type", "").lower() or \
               "octet-stream" in response.headers.get("Content-Type", "").lower() or \
               "text/x-comma-separated-values" in response.headers.get("Content-Type", "").lower():
                logger.info(f"Raw response content (first 500 bytes, as it seems to be CSV): {response.content[:500]}")
            else:
                logger.info(f"Raw response content (text, as it might not be CSV): {response.text[:1000]}")

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            is_html_error = "text/html" in content_type or (
                not ("csv" in content_type or 
                     "octet-stream" in content_type or 
                     "text/x-comma-separated-values" in content_type
                     ) and
                response.text.lstrip().startswith("<")
            )

            if is_html_error:
                logger.warning(
                    f"Attempt {attempt + 1}: Received HTML instead of CSV. "
                    f"Content-Type: {content_type}. Response snippet: {response.text[:300]}"
                )
            elif "csv" in content_type or \
                 "octet-stream" in content_type or \
                 "text/x-comma-separated-values" in content_type:
                try:
                    # JMA CSVs can sometimes have mixed encodings or control characters.
                    # Attempt decoding with 'shift-jis' as per the provided script.
                    # errors='replace' will replace problematic characters with a placeholder.
                    csv_data = response.content.decode("shift-jis", errors="replace")
                    logger.info(
                        f"Successfully downloaded data (approx. {len(response.content)} bytes). Content-Type: {content_type}"
                    )
                    # Check if decoded data seems valid (e.g., not empty, has newlines)
                    if not csv_data.strip() or ("\n" not in csv_data and "\r" not in csv_data):
                        logger.warning(f"Downloaded CSV for {payload.get('ymdList')} seems empty or malformed after decoding. Decoded head: {csv_data[:200]}")
                        # Fall through to retry logic if content seems off, despite 200 OK and CSV type
                    else:
                        # ★★★ REMOVE THIS BLOCK ★★★
                        # logger.info("Raw decoded CSV content (first 20 lines):")
                        # csv_lines = csv_data.splitlines()
                        # for i, line in enumerate(csv_lines[:20]):
                        #     logger.info(f"RAW_CSV_LINE_{i:02d}: {line}")
                        # ★★★ END REMOVE ★★★
                        return csv_data
                except UnicodeDecodeError as e:
                    logger.error(
                        f"Attempt {attempt + 1}: Failed to decode response as Shift-JIS (cp932): {e}. "
                        f"Content-Type: {content_type}."
                    )
            else: 
                 logger.warning(
                    f"Attempt {attempt + 1}: Unexpected Content-Type: {content_type}. "
                    f"Response snippet: {response.text[:300]}"
                )
        
        except requests.exceptions.HTTPError as e:
            logger.warning(
                f"Attempt {attempt + 1} HTTP error: {e}. Status: {e.response.status_code}."
                f"Response text: {e.response.text[:300] if e.response else 'N/A'}"
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} request failed: {e}")
        
        if attempt < max_retries - 1:
            delay = initial_retry_delay * (2**attempt)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            logger.error(f"Max retries reached for payload targeting ymd: {payload.get('ymdList')}")
            return None
    return None 


def main():
    """Main execution function."""
    args: Optional[argparse.Namespace] = None
    if len(sys.argv) > 1:
        logger.info("Command-line arguments detected. Parsing CLI arguments.")
        args = parse_cli_args()
    else:
        logger.info("No command-line arguments detected. Entering interactive mode.")
        args = get_interactive_args()

    if not args:
        logger.error("Argument parsing failed. Exiting.")
        return

    phpsessid_from_html: Optional[str] = None

    with requests.Session() as session:
        try:
            logger.info(f"Initializing session with GET to {JMA_INDEX_URL}...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            session.headers.update(headers)
            init_response = session.get(JMA_INDEX_URL, timeout=30, allow_redirects=True) # Ensure redirects are followed
            init_response.raise_for_status()
            
            # Log the full HTML content for debugging PHPSESSID
            logger.info(f"Initial GET request to {JMA_INDEX_URL} completed with status: {init_response.status_code}")
            logger.debug(f"Initial GET response URL (after potential redirects): {init_response.url}")
            # logger.debug(f"Initial GET response headers: {init_response.headers}")
            # logger.debug(f"Session cookies after initial GET (from session.cookies): {session.cookies.items()}")
            # logger.debug(f"Response cookies after initial GET (from init_response.cookies): {[(c.name, c.value, c.domain, c.path) for c in init_response.cookies]})

            # Output HTML to a file for inspection
            html_output_filename = "jma_index_response.html"
            with open(html_output_filename, "w", encoding="utf-8") as f_html:
                f_html.write(init_response.text)
            logger.info(f"Saved initial GET response HTML to {html_output_filename} for inspection.")

            # Attempt to parse PHPSESSID from HTML content (emulating provided script)
            soup = BeautifulSoup(init_response.text, 'lxml') # Or 'html.parser' if lxml not available
            sid_input = soup.find('input', {'id': 'sid'})
            if sid_input and sid_input.get('value'):
                phpsessid_from_html = sid_input.get('value')
                logger.info(f"Successfully extracted PHPSESSID from HTML (#sid input): {phpsessid_from_html}")
            else:
                logger.warning("Could not find input with id='sid' or it has no value in the HTML response.")
                # Fallback or error if SID is crucial and not found via cookie or HTML
                # For now, we will proceed and let the payload builder use None if not found

            # Original cookie-based PHPSESSID check (can be kept as a fallback or for logging)
            phpsessid_from_cookie = session.cookies.get('PHPSESSID')
            if phpsessid_from_cookie:
                logger.info(f"PHPSESSID also found in session.cookies: {phpsessid_from_cookie}")
                if not phpsessid_from_html: # If HTML parsing failed, but cookie exists
                    logger.info("Using PHPSESSID from cookie as HTML parsing did not yield one.")
                    phpsessid_from_html = phpsessid_from_cookie 
            elif not phpsessid_from_html: # Neither method worked
                 logger.error("PHPSESSID was not found in cookies NOR extracted from HTML.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to perform initial GET to {JMA_INDEX_URL}: {e}. Exiting.")
            return

        for station in args.station_list:
            logger.info(f"Processing station: {station}")
            for year, month, day_from, day_to in month_range(args.start_date, args.end_date):
                logger.info(
                    f"Fetching data for station {station}, Year: {year}, Month: {month:02d} (Days: {day_from}-{day_to}), Interval: {args.interval}"
                )
                
                payload = build_payload(
                    station, 
                    args.element_list, 
                    args.interval, 
                    year, 
                    month, 
                    day_from, 
                    day_to,
                    phpsessid_from_html # Use the one from HTML (or cookie if HTML failed)
                )
                
                csv_content = fetch_month_data(
                    session, payload, JMA_POST_URL, MAX_RETRIES, INITIAL_RETRY_DELAY_SECONDS
                )

                if csv_content:
                    # ★★★ ENSURE THIS BLOCK IS CORRECT ★★★
                    logger.info("Raw decoded CSV content (first 20 lines):")
                    csv_lines = csv_content.splitlines()
                    for i, line in enumerate(csv_lines[:20]):
                        logger.info(f"RAW_CSV_LINE_{i:02d}: {line}")
                    # ★★★ END ENSURE ★★★

                    # CSV文字列をDataFrameに変換
                    try:
                        df = None
                        # ヘッダーのスキップ行数や読み込むヘッダー行を指定
                        # 時別データと日別データでヘッダー構造が微妙に異なる可能性を考慮
                        if args.interval == "hourly":
                            # 時別値 CSVのヘッダー構造:
                            # 行0: ダウンロード時刻 (skip)
                            # 行1: 空行 (skip)
                            # 行2: 地点名 (例: ,東京,東京,...) (skip initially)
                            # 行3: 物理量名1 (例: 年月日時,気温(℃),気温(℃),...) <- header level 0
                            # 行4: 物理量名2 (例: ,,,,風向,風向,...) <- header level 1
                            # 行5: 品質情報など (例: ,,品質情報,均質番号,...) (drop this row after read)
                            # 行6 onwards: Data
                            
                            # Skip first 3 rows (download time, blank, station names row)
                            # Use the next 2 rows as a MultiIndex header
                            try:
                                raw_df = pd.read_csv(io.StringIO(csv_content), skiprows=3, header=[0,1], na_filter=False, skip_blank_lines=False)
                                # ★★★ Add detailed logging for raw MultiIndex columns ★★★
                                logger.info(f"RAW DataFrame columns (MultiIndex) for station {station}, {year}-{month:02d}: {list(raw_df.columns)}")

                                # Drop the first data row which contains quality info, etc.
                                if not raw_df.empty and len(raw_df) > 1: # Ensure there's more than just the quality row
                                     # Check if the first row looks like a quality/metadata row rather than actual data
                                     # A simple heuristic: if the first column of the first row is empty or non-numeric for datetime
                                     # For JMA, the first column after header processing should be datetime like '2023/04/01 01:00'
                                     # Quality rows often have empty first cells or specific keywords.
                                    first_val_first_row = str(raw_df.iloc[0, 0]).strip()
                                    if not first_val_first_row or any(kw in first_val_first_row for kw in ["品質", "均質", "現象なし"]):
                                        logger.debug(f"Dropping first row (likely quality info): {raw_df.iloc[0].to_dict()}")
                                        raw_df = raw_df.iloc[1:].reset_index(drop=True)
                                    else:
                                        logger.debug(f"First row seems like data, not dropping: {raw_df.iloc[0].to_dict()}")
                                else:
                                    logger.warning("DataFrame is empty or has only one row after initial read (hourly), cannot drop quality row.")

                                # Process MultiIndex columns
                                new_cols = []
                                for col_level0, col_level1 in raw_df.columns:
                                    col_level0_str = str(col_level0).strip()
                                    col_level1_str = str(col_level1).strip()
                                    
                                    if col_level0_str.startswith("Unnamed:"): col_level0_str = ""
                                    if col_level1_str.startswith("Unnamed:"): col_level1_str = ""

                                    # Enhanced standardization for col_level0_str, anticipating mojibake
                                    std_col_level0 = col_level0_str.replace("(℃)", "_degC") \
                                                                  .replace("(時間)", "_h") \
                                                                  .replace("(m/s)", "_mps") \
                                                                  .replace("(mm)", "_mm") \
                                                                  .replace("(hPa)", "_hPa") \
                                                                  .replace("(MJ/㎡)", "_MJ_per_m2") \
                                                                  .replace(f"(MJ/\\uFFFD\\u0075)", "_MJ_per_m2") \
                                                                  .replace("(MJ/u)", "_MJ_per_m2") \
                                                                  .replace("(cm)", "_cm") \
                                                                  .replace("(%)", "_percent") \
                                                                  .replace("()", "_percent") \
                                                                  .replace("/", "_per_") \
                                                                  .replace(" ", "_") \
                                                                  .replace("(", "") \
                                                                  .replace(")", "") \
                                                                  .replace("：", "") \
                                                                  .replace("・", "_") \
                                                                  .lower()
                                    
                                    # Standardize col_level1_str similarly, but its role is more for filtering
                                    std_col_level1 = col_level1_str.lower() # Simple lower for keyword matching

                                    final_col_name = ""
                                    # Logic to identify primary data columns vs. ancillary info (quality, etc.)
                                    # Primary data columns usually have an empty or non-descriptive col_level1_str
                                    is_primary_data_candidate = not col_level1_str or \
                                                                "unnamed" in std_col_level1 or \
                                                                all(kw not in std_col_level1 for kw in ["品質情報", "均質番号", "現象なし情報", "風向"])

                                    if ("風速" in col_level0_str or "wind_speed" in std_col_level0) and \
                                       ("風向" in col_level1_str or "wind_direction" in std_col_level1 or col_level1_str == "風向"):
                                        final_col_name = "wind_direction_intermediate"
                                    elif is_primary_data_candidate and std_col_level0:
                                        # This is likely the main data column for the physical quantity in std_col_level0
                                        final_col_name = f"{std_col_level0}_main"
                                    elif std_col_level0 and std_col_level1 : # Ancillary data or specific sub-types
                                        final_col_name = f"{std_col_level0}_{std_col_level1.replace(' ', '_')}" # Keep sub-info
                                    elif std_col_level0: # Only level 0 info
                                        final_col_name = std_col_level0
                                    elif std_col_level1: # Only level 1 info (less likely for good data)
                                        final_col_name = std_col_level1.replace(' ', '_')
                                    else:
                                        final_col_name = f"col_placeholder_{len(new_cols)}"
                                    
                                    # Ensure uniqueness by appending a counter if a collision is detected (simple version)
                                    original_final_col_name = final_col_name
                                    count = 1
                                    while final_col_name in new_cols:
                                        final_col_name = f"{original_final_col_name}_{count}"
                                        count += 1
                                    new_cols.append(final_col_name)
                                raw_df.columns = new_cols
                                # ★★★ Log columns after new_cols assignment ★★★
                                logger.info(f"PROCESSED new_cols (raw_df.columns) for station {station}, {year}-{month:02d}: {raw_df.columns.tolist()}")
                                
                                # Rename first column to 'datetime_raw'
                                if not raw_df.empty:
                                    raw_df.rename(columns={raw_df.columns[0]: 'datetime_raw'}, inplace=True)
                                    # ★★★ Log the first few values of datetime_raw before parsing ★★★
                                    if 'datetime_raw' in raw_df.columns:
                                        logger.info(f"First 5 values of datetime_raw for station {station}, {year}-{month:02d}: {raw_df['datetime_raw'].head().tolist()}")
                                else:
                                    logger.warning("Hourly DataFrame is empty before renaming datetime_raw column.")

                            except Exception as e_parse:
                                logger.error(f"Error during hourly CSV parsing for station {station}, {year}-{month:02d}: {e_parse}", exc_info=True)
                                continue


                        elif args.interval == "daily":
                            # 日別データの場合: ダウンロード時刻, 空行, 地点名, (物理量ヘッダ), 品質情報
                            # Skip first 3 rows (download time, blank, station names row)
                            # Use the next 2 rows as a MultiIndex header (main and sub-metric like '現象なし情報')
                            try:
                                raw_df = pd.read_csv(io.StringIO(csv_content), skiprows=3, header=[0,1], na_filter=False, skip_blank_lines=False)
                                # Drop the first data row which contains quality info, etc.
                                if not raw_df.empty and len(raw_df) > 1:
                                    first_val_first_row = str(raw_df.iloc[0, 0]).strip()
                                    if not first_val_first_row or any(kw in first_val_first_row for kw in ["品質", "均質", "現象なし"]):
                                        logger.debug(f"Dropping first row (likely quality info - daily): {raw_df.iloc[0].to_dict()}")
                                        raw_df = raw_df.iloc[1:].reset_index(drop=True)
                                    else:
                                        logger.debug(f"First row seems like data, not dropping (daily): {raw_df.iloc[0].to_dict()}")
                                else:
                                    logger.warning("DataFrame is empty or has only one row after initial read (daily), cannot drop quality row.")

                                # Process MultiIndex columns for daily data
                                new_cols = []
                                for col_level0, col_level1 in raw_df.columns:
                                    col_level0_str = str(col_level0).strip()
                                    col_level1_str = str(col_level1).strip()
                                    if col_level0_str.startswith("Unnamed:"): col_level0_str = ""
                                    if col_level1_str.startswith("Unnamed:"): col_level1_str = ""

                                    if col_level0_str and col_level1_str:
                                        new_cols.append(f"{col_level0_str}_{col_level1_str}")
                                    elif col_level0_str:
                                        new_cols.append(col_level0_str)
                                    elif col_level1_str:
                                        new_cols.append(col_level1_str)
                                    else:
                                        new_cols.append(f"col_{len(new_cols)}")
                                raw_df.columns = new_cols

                                raw_df = raw_df.rename(columns=lambda c: str(c).replace("(℃)", "_degC")
                                                                    .replace("(時間)", "_h")
                                                                    .replace("(m/s)", "_mps")
                                                                    .replace("(mm)", "_mm")
                                                                    .replace("(hPa)", "_hPa")
                                                                    .replace("(MJ/㎡)", "_MJ_per_m2")
                                                                    .replace("(cm)", "_cm")
                                                                    .replace("(%)", "_percent")
                                                                    .replace("合計", "total_") # e.g. total_降水量_mm
                                                                    .replace("平均", "avg_")   # e.g. avg_気温_degC
                                                                    .replace("最大", "max_")
                                                                    .replace("最小", "min_")
                                                                    .replace("/", "_per_")
                                                                    .replace(" ", "_")
                                                                    .replace("(", "")
                                                                    .replace(")", "")
                                                                    .replace("：", "")
                                                                    .replace("・", "_")
                                                                    .lower())
                                if not raw_df.empty:
                                    raw_df.rename(columns={raw_df.columns[0]: 'datetime_raw'}, inplace=True)
                                    # ★★★ Log the first few values of datetime_raw before parsing (Daily) ★★★
                                    if 'datetime_raw' in raw_df.columns:
                                        logger.info(f"First 5 values of datetime_raw for DAILY station {station}, {year}-{month:02d}: {raw_df['datetime_raw'].head().tolist()}")
                                else:
                                    logger.warning("Daily DataFrame is empty before renaming datetime_raw column.")
                            except Exception as e_parse_daily:
                                logger.error(f"Error during daily CSV parsing for station {station}, {year}-{month:02d}: {e_parse_daily}", exc_info=True)
                                continue
                        else: # monthly などは一旦エラー
                            logger.error(f"Unsupported interval for DataFrame conversion: {args.interval}")
                            continue
                        
                        df = raw_df.copy()

                        # Column mapping based on user's desired items and observed RAW columns
                        # Keys should be the result of the new_cols processing from raw_df.columns
                        column_mapping = {
                            # Expected standardized unique names from new_cols logic for main data
                            'datetime_raw': 'datetime_raw', 
                            '気温_degc_main': 'temperature',
                            '日照時間_h_main': 'sunshine_duration', 
                            '風速_mps_main': 'wind_speed',
                            'wind_direction_intermediate': 'wind_direction', 
                            '日射量mj_per_\uFFFD\u0075_main': 'global_solar_radiation', # Match logged mojibake: 日射量mj_per_u_main
                            '天気_main': 'weather_description',
                            '降雪_cm_main': 'snowfall_depth',

                            # Fallbacks if _main is not consistently applied or for simpler structures
                            '気温_degc': 'temperature',
                            '日照時間_h': 'sunshine_duration', 
                            '風速_mps': 'wind_speed',
                            '日射量mj_per_\uFFFD\u0075': 'global_solar_radiation', # Match logged mojibake: 日射量mj_per_u
                            '天気': 'weather_description',
                            '降雪_cm': 'snowfall_depth',
                        }
                        
                        # df = raw_df.rename(columns=column_mapping, errors='ignore') # Apply mapping to raw_df after its columns are processed by new_cols logic
                        # The actual renaming will use the 'df' which has 'new_cols' as columns
                        df.rename(columns=column_mapping, inplace=True, errors='ignore')
                        logger.debug(f"DataFrame columns after mapping: {df.columns.tolist()}")
                        
                        # Ensure 'datetime_raw' or 'datetime' column exists before proceeding
                        datetime_col_name = None
                        if 'datetime_raw' in df.columns:
                            datetime_col_name = 'datetime_raw'
                        elif 'datetime' in df.columns:
                            # Check if it's already datetime type
                            if pd.api.types.is_datetime64_any_dtype(df['datetime']):
                                datetime_col_name = 'datetime'
                            else:
                                # Try converting it if it exists but is not the right type
                                logger.warning("'datetime' column exists but is not datetime type. Attempting conversion.")
                                try:
                                    if args.interval == 'hourly':
                                        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                                        if df['datetime'].isna().sum() > len(df) * 0.5:
                                            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
                                    else: # daily
                                        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d', errors='coerce')
                                    datetime_col_name = 'datetime'
                                except Exception as e_conv:
                                    logger.error(f"Failed to convert existing 'datetime' column: {e_conv}")
                                    # Fall through, datetime_col_name remains None
                        
                        if not datetime_col_name and not df.empty:
                             logger.error(f"CRITICAL: Neither 'datetime_raw' nor a convertible 'datetime' column found for station {station}, {year}-{month:02d}. Columns: {df.columns.tolist()}")
                             potential_dt_cols = [col for col in df.columns if 'date' in col or 'time' in col or '年月' in col]
                             if potential_dt_cols:
                                 logger.warning(f"Attempting to use '{potential_dt_cols[0]}' as datetime source as a fallback.")
                                 datetime_col_name = potential_dt_cols[0]
                                 # Try converting this fallback column
                                 try:
                                    if args.interval == 'hourly':
                                        df[datetime_col_name + '_dt'] = pd.to_datetime(df[datetime_col_name], errors='coerce')
                                        if df[datetime_col_name + '_dt'].isna().sum() > len(df) * 0.5:
                                             df[datetime_col_name + '_dt'] = pd.to_datetime(df[datetime_col_name], format='%Y/%m/%d %H:%M:%S', errors='coerce')
                                    else: # daily
                                        df[datetime_col_name + '_dt'] = pd.to_datetime(df[datetime_col_name], format='%Y/%m/%d', errors='coerce')
                                    if df[datetime_col_name + '_dt'].notna().any():
                                        df['datetime'] = df[datetime_col_name + '_dt'] # Assign to standard 'datetime' col
                                        datetime_col_name = 'datetime' # Now use the converted one
                                    else:
                                        logger.error("Fallback datetime column conversion failed.")
                                        datetime_col_name = None # Conversion failed
                                 except Exception as e_fb_conv:
                                    logger.error(f"Fallback datetime conversion failed: {e_fb_conv}")
                                    datetime_col_name = None
                             else:
                                 logger.error("No suitable fallback datetime column identified. Skipping this file.")
                                 continue
                        
                        # Proceed only if we have a valid datetime source column identified
                        if datetime_col_name:
                            if datetime_col_name == 'datetime_raw': # If source was raw, convert it now
                                if args.interval == 'hourly':
                                    try:
                                        df['datetime'] = pd.to_datetime(df['datetime_raw'], errors='coerce') # Try inferring first
                                        if df['datetime'].isna().sum() > len(df) * 0.5:
                                             df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
                                    except Exception as e_dt:
                                        logger.error(f"Exception during hourly datetime conversion from raw: {e_dt}")
                                        df['datetime'] = pd.NaT 
                                else: # daily
                                    df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d', errors='coerce')
                                
                                # Log parsing failures
                                failed_datetime_parse_df = df[df['datetime'].isna() & df['datetime_raw'].notna()]
                                if not failed_datetime_parse_df.empty:
                                    logger.warning(f"Failed to parse datetime for {len(failed_datetime_parse_df)} rows from raw for station {station}, {year}-{month:02d}. Examples: {failed_datetime_parse_df['datetime_raw'].head().tolist()}")
                                
                                df = df.drop(columns=['datetime_raw'], errors='ignore')
                            # Now we should have a 'datetime' column of datetime64 type
                            
                            if 'datetime' in df.columns and pd.api.types.is_datetime64_any_dtype(df['datetime']):
                                # Drop rows where datetime conversion failed
                                initial_rows = len(df)
                                df = df.dropna(subset=['datetime'])
                                if len(df) < initial_rows:
                                    logger.warning(f"Dropped {initial_rows - len(df)} rows due to failed datetime conversion for {station} {year}-{month:02d}.")

                                # Create date and time columns
                                df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
                                df['time'] = df['datetime'].dt.strftime('%H:%M')
                                
                                # Create the new primary key
                                df['station_id'] = station # Ensure station_id column exists
                                df['primary_key'] = df.apply(
                                    lambda row: f"{row['station_id']}_{row['datetime'].strftime('%Y%m%d%H%M')}" if pd.notna(row['datetime']) else None, axis=1
                                )
                                
                                # Drop original datetime column and old master_key if it exists
                                df = df.drop(columns=['datetime', 'master_key'], errors='ignore')
                                
                            else:
                                logger.error(f"Final 'datetime' column is missing or not datetime type after processing for {station} {year}-{month:02d}. Cannot create date/time/primary_key.")
                                df = pd.DataFrame() # Make DF empty if essential steps failed
                        else: 
                            # If no valid datetime source was found even after fallback
                            logger.error(f"No valid datetime source identified for {station} {year}-{month:02d}. Skipping file.")
                            df = pd.DataFrame() # Make DF empty

                        # If df became empty due to errors, skip further processing for this file
                        if df.empty:
                            logger.warning(f"DataFrame is empty for station {station}, {year}-{month:02d}. Skipping processing and save.") # Adjusted log
                            continue
                        
                        # --- Apply Sin/Cos Calculation --- 
                        # Moved here: Call after basic processing and before final column selection
                        if 'wind_direction' in df.columns:
                            logger.info("Calling calculate_wind_components...")
                            if not df.empty:
                                logger.debug(f"Sample wind_direction input: {df['wind_direction'].head().tolist()}")
                            df = calculate_wind_components(df) # This adds sin/cos columns
                            logger.info("Finished calculate_wind_components.")
                            logger.debug(f"Columns after calculate_wind_components: {df.columns.tolist()}")
                        else:
                            logger.warning("'wind_direction' column not found before sin/cos calculation. Adding empty sin/cos columns.")
                            df['wind_direction_sin'] = pd.NA
                            df['wind_direction_cos'] = pd.NA

                        # Define FINAL required columns based on user's schema
                        required_cols_final_schema = [
                            'primary_key', 'station_id', 'date', 'time', 'temperature', 
                            'sunshine_duration', 'global_solar_radiation', 'wind_speed', 
                            'wind_direction_sin', 'wind_direction_cos', 
                            'weather_description', 'snowfall_depth'
                        ]
                        
                        # Ensure all required columns exist, adding missing ones with NA if necessary
                        for col in required_cols_final_schema:
                            if col not in df.columns:
                                logger.warning(f"Required column '{col}' missing after processing. Adding column with NA values.")
                                df[col] = pd.NA 
                        
                        # Select and reorder columns *strictly* according to the final schema ONCE
                        logger.info(f"Selecting final columns: {required_cols_final_schema}")
                        # Ensure we only select columns that actually exist in the df at this point
                        final_columns_present = [col for col in required_cols_final_schema if col in df.columns]
                        df = df[final_columns_present]

                        # Convert numeric types (Moved after final column selection)
                        numeric_cols_to_convert = [
                            'temperature', 'sunshine_duration', 'global_solar_radiation', 
                            'wind_speed', 'wind_direction_sin', 'wind_direction_cos', 
                            'snowfall_depth' 
                        ]

                        for col in numeric_cols_to_convert:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            else:
                                # This case should be less likely now due to the check above, but kept for safety
                                logger.debug(f"Numeric conversion: Column '{col}' not found in final DataFrame. Skipping.")

                        # Round sin/cos columns to 2 decimal places
                        if 'wind_direction_sin' in df.columns:
                            df['wind_direction_sin'] = df['wind_direction_sin'].round(2)
                        if 'wind_direction_cos' in df.columns:
                            df['wind_direction_cos'] = df['wind_direction_cos'].round(2)

                        logger.info(f"Final DataFrame for station {station}, {year}-{month:02d} ({args.interval}) ready. Columns: {df.columns.tolist()}")
                        if not df.empty:
                            logger.debug(f"Final DataFrame head:\n{df.head().to_string()}")

                            # Import data into DuckDB unless skipped
                            if not args.skip_db_import:
                                logger.info(f"Attempting to import data into DuckDB (DB path from args: {args.db_path}) for station {station}, {year}-{month:02d}...")
                                try:
                                    # Use context manager for the importer to ensure connection closing
                                    # JMAWeatherDBImporter will use DuckDBConnection, which resolves None to .env path
                                    with JMAWeatherDBImporter(db_path=args.db_path) as importer:
                                        inserted_count = importer.import_dataframe(df)
                                        logger.info(f"Successfully imported {inserted_count} rows into DuckDB table 'jma_weather'.")
                                except Exception as e:
                                    logger.error(f"Failed to import data into DuckDB for station {station}, {year}-{month:02d}: {e}", exc_info=True)
                            else:
                                logger.info("Skipping database import as per --skip-db-import flag.")
                        else:
                            logger.warning("Created DataFrame is empty.")

                    except Exception as e:
                        # Log error for the specific month/station but continue with others
                        logger.error(f"Error processing data for station {station}, month {year}-{month:02d}: {e}", exc_info=True)
                        continue # 次のファイルの処理へ
                else:
                    logger.error(
                        f"Failed to fetch data for station {station}, {year}-{month:02d} after retries."
                    )
                
                logger.info(f"Waiting for {args.rate} seconds before next request...")
                time.sleep(args.rate)
    
    logger.info("JMA historical data download and CSV generation finished.") # Adjusted log message
    # No explicit return of DataFrame from main, script focuses on CSV saving.

if __name__ == "__main__":
    main() 