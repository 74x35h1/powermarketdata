import logging
import json
import time
import io
import math
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Import constants from jma_config
from .jma_config import (
    WIND_DIRECTION_TO_DEGREES,
    JMA_POST_URL,
    MAX_RETRIES,
    INITIAL_RETRY_DELAY_SECONDS
)

logger = logging.getLogger(__name__)

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
    
    if 'wind_speed' in df.columns:
        df.loc[df['wind_speed'] == 0, 'wind_direction_sin'] = df.loc[df['wind_speed'] == 0, 'wind_direction_sin'].fillna(0.0)
        df.loc[df['wind_speed'] == 0, 'wind_direction_cos'] = df.loc[df['wind_speed'] == 0, 'wind_direction_cos'].fillna(0.0)

    df.drop(columns=['wind_angle_deg'], inplace=True, errors='ignore')
    logger.info("Added 'wind_direction_sin' and 'wind_direction_cos' columns.")
    return df

def build_jma_payload(
    station_code: str,
    element_codes: List[str],
    interval: str, # "hourly", "daily", etc.
    year: int,
    month: int,
    day_from: int,
    day_to: int,
    phpsessid: Optional[str],
) -> Dict[str, Any]:
    """
    Builds the payload for the POST request to JMA.
    """
    aggrg_period_map = {"monthly": 0, "daily": 1, "hourly": 9} 

    payload = {
        "stationNumList": json.dumps([f"s{station_code}"]),
        "elementNumList": json.dumps([[str(c), ""] for c in element_codes]),
        "ymdList": json.dumps([year, year, month, month, day_from, day_to]),
        "aggrgPeriod": aggrg_period_map.get(interval, 9), # Default to hourly (9) if interval not in map
        "csvFlag": 1,
        "rmkFlag": 1,
        "disconnectFlag": 1,
        "ymdLiteral": 1,
        "youbiFlag": 0,
        "kijiFlag": 0,
        "jikantaiFlag": 0,
        "jikantaiList": json.dumps([1, 24]),
        "interAnnualFlag": 1,
        "optionNumList": [],
        "downloadFlag": "true",
        "huukouFlag": 0,
    }
    
    if phpsessid:
        payload["PHPSESSID"] = phpsessid
    else:
        logger.warning("PHPSESSID not available for JMA payload.")
        
    logger.debug(f"Built JMA payload: {payload}")
    return payload

def fetch_jma_csv_data(
    session: requests.Session,
    payload: Dict[str, Any],
) -> Optional[str]:
    """
    Fetches JMA data as a CSV string for a single request with retries.
    Uses constants for URL, retries, and delay from jma_config.
    """
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Attempt {attempt + 1}/{MAX_RETRIES} to POST to {JMA_POST_URL} with payload subset: "
                        f"station: {payload.get('stationNumList')}, period: {payload.get('aggrgPeriod')}, "
                        f"ymd: {payload.get('ymdList')}")
            # logger.debug(f"Full payload being sent: {json.dumps(payload, ensure_ascii=False)}") # Can be verbose

            response = session.post(JMA_POST_URL, data=payload, timeout=60)
            logger.debug(f"Response headers: {response.headers}")

            # Log raw content for debugging if needed
            # if "csv" in response.headers.get("Content-Type", "").lower() or \
            #    "octet-stream" in response.headers.get("Content-Type", "").lower():
            #     logger.debug(f"Raw response content (first 500 bytes, CSV type): {response.content[:500]}")
            # else:
            #     logger.debug(f"Raw response content (text, non-CSV type): {response.text[:1000]}")

            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            
            is_html_error = "text/html" in content_type or (
                not ("csv" in content_type or 
                     "octet-stream" in content_type or 
                     "text/x-comma-separated-values" in content_type) and
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
                    csv_data = response.content.decode("shift-jis", errors="replace")
                    logger.info(
                        f"Successfully downloaded data (approx. {len(response.content)} bytes). Content-Type: {content_type}"
                    )
                    if not csv_data.strip() or ("\n" not in csv_data and "\r" not in csv_data):
                        logger.warning(f"Downloaded CSV for {payload.get('ymdList')} seems empty or malformed after decoding. Head: {csv_data[:200]}")
                    else:
                        return csv_data
                except UnicodeDecodeError as e:
                    logger.error(
                        f"Attempt {attempt + 1}: Failed to decode response as Shift-JIS: {e}. Content-Type: {content_type}."
                    )
            else: 
                 logger.warning(
                    f"Attempt {attempt + 1}: Unexpected Content-Type: {content_type}. Response snippet: {response.text[:300]}"
                )
        
        except requests.exceptions.HTTPError as e:
            logger.warning(
                f"Attempt {attempt + 1} HTTP error: {e}. Status: {e.response.status_code if e.response else 'N/A'}. "
                f"Response: {e.response.text[:300] if e.response else 'N/A'}"
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} request failed: {e}")
        
        if attempt < MAX_RETRIES - 1:
            delay = INITIAL_RETRY_DELAY_SECONDS * (2**attempt)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            logger.error(f"Max retries reached for JMA data fetch. Payload ymd: {payload.get('ymdList')}")
            return None
    return None

def parse_jma_csv(csv_content: str, station_id: str, year: int, month: int, interval: str) -> Optional[pd.DataFrame]:
    """
    Parses the raw CSV string content from JMA into a cleaned Pandas DataFrame.
    Includes column renaming, type conversion, and primary key generation.
    Returns a DataFrame or None if parsing fails significantly.
    """
    logger.info(f"Parsing JMA CSV for station {station_id}, {year}-{month:02d}, interval: {interval}")
    try:
        df = None
        raw_df = None # Initialize raw_df

        if interval == "hourly":
            try:
                raw_df = pd.read_csv(io.StringIO(csv_content), skiprows=3, header=[0,1], na_filter=False, skip_blank_lines=False)
                logger.debug(f"RAW Hourly DataFrame columns (MultiIndex) for station {station_id}, {year}-{month:02d}: {list(raw_df.columns)}")

                if not raw_df.empty and len(raw_df) > 1:
                    first_val_first_row = str(raw_df.iloc[0, 0]).strip()
                    if not first_val_first_row or any(kw in first_val_first_row for kw in ["品質", "均質", "現象なし"]):
                        logger.debug(f"Dropping first row (likely quality info): {raw_df.iloc[0].to_dict()}")
                        raw_df = raw_df.iloc[1:].reset_index(drop=True)
                    else:
                        logger.debug(f"First row seems like data, not dropping: {raw_df.iloc[0].to_dict()}")
                elif not raw_df.empty:
                     logger.debug("Hourly DataFrame has only one row, not dropping.")
                else:
                    logger.warning("Hourly DataFrame is empty after initial read, cannot drop quality row.")

                # Process MultiIndex columns
                new_cols = []
                if not raw_df.empty:
                    for col_level0, col_level1 in raw_df.columns:
                        col_level0_str = str(col_level0).strip()
                        col_level1_str = str(col_level1).strip()
                        
                        if col_level0_str.startswith("Unnamed:"): col_level0_str = ""
                        if col_level1_str.startswith("Unnamed:"): col_level1_str = ""

                        std_col_level0 = col_level0_str.replace("(℃)", "_degC") \
                                                      .replace("(時間)", "_h") \
                                                      .replace("(m/s)", "_mps") \
                                                      .replace("(mm)", "_mm") \
                                                      .replace("(hPa)", "_hPa") \
                                                      .replace("(MJ/㎡)", "_MJ_per_m2") \
                                                      .replace(f"(MJ/\uFFFD\u0075)", "_MJ_per_m2") \
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
                        
                        std_col_level1 = col_level1_str.lower()

                        final_col_name = ""
                        is_primary_data_candidate = not col_level1_str or \
                                                    "unnamed" in std_col_level1 or \
                                                    all(kw not in std_col_level1 for kw in ["品質情報", "均質番号", "現象なし情報", "風向"])

                        if ("風速" in col_level0_str or "wind_speed" in std_col_level0) and \
                           ("風向" in col_level1_str or "wind_direction" in std_col_level1 or col_level1_str == "風向"):
                            final_col_name = "wind_direction_intermediate"
                        elif is_primary_data_candidate and std_col_level0:
                            final_col_name = f"{std_col_level0}_main"
                        elif std_col_level0 and std_col_level1 :
                            final_col_name = f"{std_col_level0}_{std_col_level1.replace(' ', '_')}"
                        elif std_col_level0:
                            final_col_name = std_col_level0
                        elif std_col_level1:
                            final_col_name = std_col_level1.replace(' ', '_')
                        else:
                            final_col_name = f"col_placeholder_{len(new_cols)}"
                        
                        original_final_col_name = final_col_name
                        count = 1
                        while final_col_name in new_cols:
                            final_col_name = f"{original_final_col_name}_{count}"
                            count += 1
                        new_cols.append(final_col_name)
                    raw_df.columns = new_cols
                    logger.debug(f"PROCESSED new_cols (hourly raw_df.columns): {raw_df.columns.tolist()}")
                    
                    if not raw_df.empty:
                        raw_df.rename(columns={raw_df.columns[0]: 'datetime_raw'}, inplace=True)
                        logger.debug(f"First 5 values of datetime_raw (hourly): {raw_df['datetime_raw'].head().tolist() if 'datetime_raw' in raw_df.columns else 'N/A'}")
                else:
                     logger.warning("Hourly raw_df is empty before column processing.")


            except Exception as e_parse_hourly:
                logger.error(f"Error during hourly CSV parsing for {station_id}, {year}-{month:02d}: {e_parse_hourly}", exc_info=True)
                return None

        elif interval == "daily":
            try:
                raw_df = pd.read_csv(io.StringIO(csv_content), skiprows=3, header=[0,1], na_filter=False, skip_blank_lines=False)
                logger.debug(f"RAW Daily DataFrame columns (MultiIndex) for station {station_id}, {year}-{month:02d}: {list(raw_df.columns)}")
                if not raw_df.empty and len(raw_df) > 1:
                    first_val_first_row = str(raw_df.iloc[0, 0]).strip()
                    if not first_val_first_row or any(kw in first_val_first_row for kw in ["品質", "均質", "現象なし"]):
                        logger.debug(f"Dropping first row (likely quality info - daily): {raw_df.iloc[0].to_dict()}")
                        raw_df = raw_df.iloc[1:].reset_index(drop=True)
                    else:
                        logger.debug(f"First row seems like data, not dropping (daily): {raw_df.iloc[0].to_dict()}")
                elif not raw_df.empty:
                     logger.debug("Daily DataFrame has only one row, not dropping.")
                else:
                    logger.warning("Daily DataFrame is empty after initial read, cannot drop quality row.")

                new_cols = []
                if not raw_df.empty:
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
                                                                .replace("合計", "total_")
                                                                .replace("平均", "avg_")
                                                                .replace("最大", "max_")
                                                                .replace("最小", "min_")
                                                                .replace("/", "_per_")
                                                                .replace(" ", "_")
                                                                .replace("(", "")
                                                                .replace(")", "")
                                                                .replace("：", "")
                                                                .replace("・", "_")
                                                                .lower())
                    logger.debug(f"PROCESSED new_cols (daily raw_df.columns): {raw_df.columns.tolist()}")
                    if not raw_df.empty:
                        raw_df.rename(columns={raw_df.columns[0]: 'datetime_raw'}, inplace=True)
                        logger.debug(f"First 5 values of datetime_raw (daily): {raw_df['datetime_raw'].head().tolist() if 'datetime_raw' in raw_df.columns else 'N/A'}")
                else:
                    logger.warning("Daily raw_df is empty before column processing.")

            except Exception as e_parse_daily:
                logger.error(f"Error during daily CSV parsing for {station_id}, {year}-{month:02d}: {e_parse_daily}", exc_info=True)
                return None
        else:
            logger.error(f"Unsupported interval for DataFrame conversion: {interval}")
            return None
        
        if raw_df is None or raw_df.empty:
            logger.warning(f"Raw DataFrame is empty or None for station {station_id}, {year}-{month:02d} after initial parsing stage. Interval: {interval}")
            return pd.DataFrame() # Return empty df if nothing to process

        df = raw_df.copy()

        column_mapping = {
            'datetime_raw': 'datetime_raw', 
            '気温_degc_main': 'temperature', 'avg_気温_degc': 'temperature', '気温_degc': 'temperature',
            '日照時間_h_main': 'sunshine_duration', 'total_日照時間_h': 'sunshine_duration', '日照時間_h': 'sunshine_duration',
            '風速_mps_main': 'wind_speed', 'avg_風速_mps': 'wind_speed', '風速_mps': 'wind_speed',
            'wind_direction_intermediate': 'wind_direction',
            '全天日射量_mj_per_m2_main': 'global_solar_radiation', 
            '日射量_mj_per_m2_main': 'global_solar_radiation',
            '全天日射量_main': 'global_solar_radiation',
            '日射量_main': 'global_solar_radiation',
            'total_全天日射量_mj_per_m2': 'global_solar_radiation',
            '天気_main': 'weather_description', '天気概況_main': 'weather_description', '天気': 'weather_description',
            '降雪_cm_main': 'snowfall_depth', '降雪_cm': 'snowfall_depth', '積雪_cm_main': 'snowfall_depth', '積雪_cm': 'snowfall_depth',
            '降水量_mm_main': 'precipitation', 'total_降水量_mm': 'precipitation', '降水量_mm': 'precipitation',
        }
        
        df.rename(columns=column_mapping, inplace=True, errors='ignore')
        logger.debug(f"DataFrame columns after mapping: {df.columns.tolist()}")
        
        datetime_col_name = None
        if 'datetime_raw' in df.columns:
            datetime_col_name = 'datetime_raw'
        
        if not datetime_col_name and not df.empty:
            logger.error(f"CRITICAL: 'datetime_raw' column not found for {station_id}, {year}-{month:02d}. Cols: {df.columns.tolist()}")
            potential_dt_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower() or '年月' in col]
            if potential_dt_cols:
                logger.warning(f"Attempting to use '{potential_dt_cols[0]}' as datetime_raw source.")
                df.rename(columns={potential_dt_cols[0]: 'datetime_raw'}, inplace=True)
                datetime_col_name = 'datetime_raw'
            else:
                logger.error("No suitable fallback datetime column. Skipping this file's parsing.")
                return pd.DataFrame() 

        if datetime_col_name:
            if interval == 'hourly':
                try:
                    df['datetime'] = pd.to_datetime(df['datetime_raw'], errors='coerce')
                    if df['datetime'].isna().sum() > len(df) * 0.5: # If many failed, try specific format
                         df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d %H:%M', errors='coerce') # Corrected format
                except Exception as e_dt_hourly: # Catch any exception during conversion
                    logger.error(f"Exception during hourly datetime conversion from raw '{df['datetime_raw'].iloc[0] if not df.empty else 'N/A'}': {e_dt_hourly}")
                    df['datetime'] = pd.NaT 
            else: # daily
                df['datetime'] = pd.to_datetime(df['datetime_raw'], format='%Y/%m/%d', errors='coerce')
            
            failed_datetime_parse_count = df['datetime'].isna().sum()
            if failed_datetime_parse_count > 0 and 'datetime_raw' in df.columns:
                failed_examples = df.loc[df['datetime'].isna() & df['datetime_raw'].notna(), 'datetime_raw'].head().tolist()
                logger.warning(f"Failed to parse datetime for {failed_datetime_parse_count} rows from raw for {station_id}, {year}-{month:02d}. Examples: {failed_examples}")
            
            df = df.drop(columns=['datetime_raw'], errors='ignore')
        
        if 'datetime' not in df.columns or not pd.api.types.is_datetime64_any_dtype(df['datetime']):
            logger.error(f"Final 'datetime' column is missing or not datetime type for {station_id}. Cannot create date/time/primary_key.")
            return pd.DataFrame() 
            
        initial_rows = len(df)
        df = df.dropna(subset=['datetime'])
        if len(df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(df)} rows due to NaT datetime values for {station_id} {year}-{month:02d}.")

        if df.empty:
            logger.warning(f"DataFrame became empty after NaT drop for {station_id} {year}-{month:02d}.")
            return pd.DataFrame()

        df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df['time'] = df['datetime'].dt.strftime('%H:%M') if interval == 'hourly' else '00:00' # Daily data has no specific time
        
        df['station_id'] = station_id
        df['primary_key'] = df.apply(
            lambda row: f"{row['station_id']}_{row['datetime'].strftime('%Y%m%d%H%M')}" if pd.notna(row['datetime']) else None, axis=1
        )
        df = df.drop(columns=['datetime'], errors='ignore')
        
        if 'wind_direction' in df.columns:
            df = calculate_wind_components(df)
        else:
            logger.debug("'wind_direction' column not found before sin/cos, adding empty sin/cos.")
            df['wind_direction_sin'] = pd.NA
            df['wind_direction_cos'] = pd.NA

        required_cols_final_schema = [
            'primary_key', 'station_id', 'date', 'time', 'temperature', 
            'sunshine_duration', 'global_solar_radiation', 'wind_speed', 
            'wind_direction_sin', 'wind_direction_cos', 
            'weather_description', 'snowfall_depth', 'precipitation'
        ]
        
        for col in required_cols_final_schema:
            if col not in df.columns:
                df[col] = pd.NA 
        
        final_columns_present = [col for col in required_cols_final_schema if col in df.columns]
        df = df[final_columns_present]

        numeric_cols_to_convert = [
            'temperature', 'sunshine_duration', 'global_solar_radiation', 
            'wind_speed', 'wind_direction_sin', 'wind_direction_cos', 
            'snowfall_depth', 'precipitation'
        ]
        for col in numeric_cols_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'wind_direction_sin' in df.columns: df['wind_direction_sin'] = df['wind_direction_sin'].round(4)
        if 'wind_direction_cos' in df.columns: df['wind_direction_cos'] = df['wind_direction_cos'].round(4)

        logger.info(f"Successfully parsed JMA CSV to DataFrame for {station_id}, {year}-{month:02d}. Shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Unhandled error in parse_jma_csv for {station_id}, {year}-{month:02d}: {e}", exc_info=True)
        return None 