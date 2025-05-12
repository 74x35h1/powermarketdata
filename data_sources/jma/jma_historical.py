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

import requests # From requirements.txt
from bs4 import BeautifulSoup # Added for HTML parsing
import pandas as pd # Added for DataFrame manipulation
# from db.duckdb_connection import DuckDBConnection # Added for DB connection

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
        required=True,
        help="Comma-separated 5-digit station codes (e.g., 47662,47626).",
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
    args = parser.parse_args()

    # Validate and convert arguments
    try:
        args.start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
        args.end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        parser.error("Dates must be in YYYY-MM-DD format.")

    if args.end_date < args.start_date:
        parser.error("End date must be after or the same as start date.")

    args.station_list = [s.strip() for s in args.stations.split(",")]
    args.element_list = [e.strip() for e in args.elements.split(",")]
    args.interval = "hourly"
    
    return args

def get_interactive_args() -> argparse.Namespace:
    """Gets arguments interactively from the user, with defaults."""
    print("Entering interactive mode for JMA historical data downloader.")
    print("Data will be fetched at an hourly interval.")
    print("Press Enter to use the default value shown in (parentheses).")

    # Default values
    default_stations_str = "47662"
    default_elements_str = "201,401,301,610,703,503"
    
    today = datetime.date.today()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    default_start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
    default_end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")
    default_outdir = "./jma_data_csv"
    default_rate = 1.2

    stations_str_in = input(f"Enter station codes (e.g., 47662,47626) (default: {default_stations_str}): ")
    stations_str = stations_str_in.strip() if stations_str_in.strip() else default_stations_str
    station_list = [s.strip() for s in stations_str.split(",")]
    while not all(s.isdigit() and len(s) == 5 for s in station_list if s):
        print("Invalid station code format. Please use 5-digit numbers, comma-separated.")
        stations_str_in = input(f"Enter station codes (default: {default_stations_str}): ")
        stations_str = stations_str_in.strip() if stations_str_in.strip() else default_stations_str
        station_list = [s.strip() for s in stations_str.split(",")]
        if not stations_str:
            station_list = [s.strip() for s in default_stations_str.split(",")]

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
        rate=rate
    )
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
                        if 'datetime_raw' not in df.columns and 'datetime' not in df.columns and not df.empty:
                             logger.error(f"CRITICAL: Neither 'datetime_raw' nor 'datetime' column found after mapping for station {station}, {year}-{month:02d}. Columns: {df.columns.tolist()}")
                             # Attempt to identify a datetime-like column if 'datetime_raw' is missing
                             potential_dt_cols = [col for col in df.columns if 'date' in col or 'time' in col or '年月' in col]
                             if potential_dt_cols:
                                 logger.warning(f"Attempting to use '{potential_dt_cols[0]}' as datetime_raw as a fallback.")
                                 df.rename(columns={potential_dt_cols[0]: 'datetime_raw'}, inplace=True)
                             else:
                                 logger.error("No suitable fallback datetime column identified. Skipping this file.")
                                 continue


                        if 'datetime_raw' in df.columns:
                            # JMA hourly data is 'YYYY/MM/DD HH:MM' (e.g., 2023/04/01 01:00) or YYYY/M/D H:MM:SS
                            # JMA daily data is 'YYYY/MM/DD'
                            if args.interval == 'hourly':
                                # Try to infer or use a more flexible format for hourly data based on new log
                                # Log shows: '2025/4/1 1:00:00' - month, day, hour might not be zero-padded, seconds are present
                                try:
                                    df['datetime'] = pd.to_datetime(df['datetime_raw'], errors='coerce') # Try inferring first
                                    # If inference fails for many, try specific format that handles non-padded month/day/hour
                                    # Note: %-m, %-d, %-H are platform-dependent (Linux/macOS).
                                    # For broader compatibility, consider custom parsing or replacing spaces around H:M:S if needed.
                                    # Given the log example '2025/4/1 1:00:00', this format should work:
                                    df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
                                except Exception as e_dt:
                                    logger.error(f"Exception during hourly datetime conversion: {e_dt}")
                                    df['datetime'] = pd.NaT # Ensure column exists with NaT if all attempts fail

                            else: # daily
                                df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d', errors='coerce')
                            
                            # ★★★ Log rows that failed datetime parsing ★★★
                            failed_datetime_parse_df = df[df['datetime'].isna() & df['datetime_raw'].notna()]
                            if not failed_datetime_parse_df.empty:
                                logger.warning(f"Failed to parse datetime for {len(failed_datetime_parse_df)} rows for station {station}, {year}-{month:02d}. Examples of unparsed datetime_raw: {failed_datetime_parse_df['datetime_raw'].head().tolist()}")

                            df = df.drop(columns=['datetime_raw'])
                        elif 'datetime' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                            # If 'datetime' column exists but is not datetime type, try to convert it
                            logger.warning("'datetime' column exists but is not datetime type. Attempting conversion.")
                            if args.interval == 'hourly':
                                try:
                                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce') # Try inferring first
                                    if df['datetime'].isna().sum() > len(df) * 0.5:
                                        logger.warning("Inferring datetime format failed for many rows (secondary path), trying specific formats for hourly data.")
                                        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
                                except Exception as e_dt_secondary:
                                    logger.error(f"Exception during hourly datetime conversion (secondary path): {e_dt_secondary}")
                                    if 'datetime' in df.columns: # Check if column still exists
                                       df['datetime'] = pd.NaT
                                    else: # Should not happen, but as a safeguard
                                       df = df.assign(datetime=pd.NaT)
                            else: # daily
                                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d', errors='coerce')
                            
                            # ★★★ Log rows that failed datetime parsing (secondary path) ★★★
                            failed_datetime_parse_df_secondary = df[df['datetime'].isna()] # Assuming original column was already named 'datetime'
                            if not failed_datetime_parse_df_secondary.empty:
                                logger.warning(f"Failed to parse datetime (secondary path) for {len(failed_datetime_parse_df_secondary)} rows for station {station}, {year}-{month:02d}. Original values might not be available if 'datetime_raw' was not present.")

                        # Re-evaluate this drop: if all fail, df becomes empty.
                        if df['datetime'].isna().all() and not df.empty:
                            logger.error(f"All datetime conversions failed for station {station}, {year}-{month:02d}. DataFrame will be empty. Check datetime format and raw values.")
                            # df = pd.DataFrame() # Make it empty to prevent downstream errors, or handle as error
                        else:
                            df = df.dropna(subset=['datetime'])

                        # Define required columns based on user's *actual* desired items
                        required_cols_standard = [
                            'datetime', 
                            'temperature', 
                            'sunshine_duration', 
                            'wind_speed', 
                            'wind_direction', 
                            'global_solar_radiation', 
                            'weather_description', 
                            'snowfall_depth'
                        ]
                        # No longer including: 'precipitation', 'local_air_pressure', 'relative_humidity'

                        select_cols = [col for col in df.columns if col in required_cols_standard or col in ['station_id', 'interval', 'master_key']]
                        
                        # Ensure all available mapped data based on the refined column_mapping are kept.
                        present_mapped_cols = [col for mapped_key in column_mapping if (col := column_mapping[mapped_key]) in df.columns and col != 'datetime_raw']
                        select_cols = list(set(['datetime'] + present_mapped_cols + ['station_id', 'interval', 'master_key']))
                        select_cols = [c for c in select_cols if c in df.columns] # Keep only those actually in df

                        missing_cols = set(required_cols_standard) - set(select_cols)
                        # Optional cols are less critical if missing
                        optional_cols = {'weather_description', 'snowfall_depth'} # Global solar can also be optional
                        missing_cols -= optional_cols

                        if missing_cols:
                            logger.warning(f"For station {station}, {year}-{month:02d}, potentially missing expected data columns after processing: {missing_cols}. Selected for output: {select_cols}. All available: {df.columns.tolist()}")
                        
                        if not 'datetime' in select_cols and not df.empty:
                             logger.error(f"CRITICAL: 'datetime' column is missing from select_cols for station {station}, {year}-{month:02d} but df is not empty. This should not happen.")
                             # Fallback to keep all columns if datetime is somehow lost, to aid debugging.
                             # However, this might lead to errors later if schema is strict.
                             # df = df # No change, keep all columns to see what went wrong.
                        elif not df.empty:
                            df = df[select_cols]
                        else:
                            # df is empty, select_cols might be just ['datetime'] or empty if all rows failed datetime conversion
                            # Create an empty df with expected columns if df itself is empty.
                            if df.empty:
                                logger.warning(f"DataFrame became empty for {station} {year}-{month:02d} (likely all rows failed datetime conversion). Creating empty frame with expected columns.")
                                df = pd.DataFrame(columns=select_cols) # Ensure schema consistency for empty data.                        

                        df['station_id'] = station
                        df['interval'] = args.interval

                        # master_key の作成 (station_id, datetime, interval)
                        if 'datetime' in df.columns and pd.api.types.is_datetime64_any_dtype(df['datetime']):
                            df['master_key'] = df.apply(
                                lambda row: f"{row['station_id']}_{row['datetime'].strftime('%Y%m%d%H%M%S')}_{row['interval']}" if pd.notna(row['datetime']) else None, axis=1
                            )
                            df = df.dropna(subset=['master_key']) # Drop rows where master_key could not be formed
                        else:
                            logger.error(f"Cannot create 'master_key' due to missing or invalid 'datetime' column for {station} {year}-{month:02d}.")
                            # Create an empty master_key column to prevent errors if df is not empty
                            if 'master_key' not in df.columns : df['master_key'] = None


                        # 数値カラムを数値型に変換 (ensure these are the final mapped names)
                        numeric_cols_final = [
                            'temperature', 'sunshine_duration', 'wind_speed', 
                            'global_solar_radiation', 'snowfall_depth' 
                            # wind_direction is handled separately if numeric, weather_description is text
                        ]
                        # if 'wind_direction' in df.columns: # wind_direction might be numeric (deg) or categorical
                        #      df['wind_direction'] = pd.to_numeric(df['wind_direction'], errors='coerce') # <-- REMOVE THIS LINE OR COMMENT IT OUT

                        for col in numeric_cols_final:
                            if col in df.columns and not df.empty:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            elif not df.empty and col not in df.columns:
                                logger.debug(f"Numeric conversion: Column '{col}' not found in DataFrame. Skipping.")
                            # If df is empty, do nothing for this column

                        # 最終的なカラム順序をスキーマに合わせて整える
                        final_cols_order_base = ['master_key', 'station_id', 'datetime', 'interval']
                        data_cols_ordered = [
                            'temperature', 'sunshine_duration', 'wind_speed', 'wind_direction',
                            'global_solar_radiation', 'weather_description', 'snowfall_depth'
                        ]
                        
                        final_cols_order = final_cols_order_base + [col for col in data_cols_ordered if col in df.columns]
                        # Add any other columns that might have been parsed but are not in the predefined order
                        additional_parsed_cols = [col for col in df.columns if col not in final_cols_order]
                        final_cols_order.extend(additional_parsed_cols)
                        
                        df = df[[col for col in final_cols_order if col in df.columns]] # Reorder and ensure only existing columns

                        logger.info(f"DataFrame for station {station}, {year}-{month:02d} ({args.interval}) processed. Columns: {df.columns.tolist()}")
                        if not df.empty:
                            logger.debug(f"DataFrame head:\\n{df.head().to_string()}")

                            # CSVファイル出力処理
                            if args.outdir:
                                os.makedirs(args.outdir, exist_ok=True)
                                filename = f"jma_weather_{station}_{year}_{month:02d}_{args.interval}.csv"
                                filepath = os.path.join(args.outdir, filename)
                                try:
                                    df.to_csv(filepath, index=False, encoding='utf-8-sig')
                                    logger.info(f"Successfully saved data to {filepath}")
                                except Exception as e:
                                    logger.error(f"Failed to save CSV to {filepath}: {e}")

                            # DB格納処理 (DuckDBConnectionを使用)
                            # db_path = None # または args.db_path などで指定
                            # with DuckDBConnection(db_path) as db:
                            #     db.save_dataframe(df, 'jma_weather', check_duplicate_master_key=True)
                            # logger.info(f"Saved {len(df)} rows to jma_weather for {station} {year}-{month:02d}")
                        else:
                            logger.warning("Created DataFrame is empty.")

                    except Exception as e:
                        logger.error(f"Error processing CSV for station {station}, {year}-{month:02d}: {e}", exc_info=True)
                        continue # 次のファイルの処理へ
                else:
                    logger.error(
                        f"Failed to fetch data for station {station}, {year}-{month:02d} after retries."
                    )
                
                logger.info(f"Waiting for {args.rate} seconds before next request...")
                time.sleep(args.rate)
    
    logger.info("JMA data download process completed.")

if __name__ == "__main__":
    main() 