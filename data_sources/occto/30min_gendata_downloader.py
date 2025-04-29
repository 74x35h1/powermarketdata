#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone OCCTO Data Downloader

This module provides a simple standalone version for downloading and displaying OCCTO data.
"""

import os
import logging
import requests
import pandas as pd
import time
import random
from datetime import date, datetime, timedelta
from pathlib import Path
import argparse
import json
# Import the new DB importer class
from data_sources.occto.db_importer import OCCTO30MinDBImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class OCCTOStandaloneDownloader:
    """
    Standalone class for downloading and displaying data from OCCTO
    """
    
    def __init__(self):
        """
        Initialize the OCCTO downloader.
        """
        # Base URLs
        self.agreement_url = "https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks"
        self.download_url = "https://hatsuden-kokai.occto.or.jp/hks-web-public/download/downloadCsv"
        
        # Create a session to maintain cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': self.agreement_url
        })
        
        # Create a directory for temporary data storage
        self.temp_dir = Path("./temp")
        self.temp_dir.mkdir(exist_ok=True)
        
        # Flag to track if agreement was confirmed
        self.agreement_confirmed = False
    
    def _random_sleep(self, min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
        """Sleep for a random amount of time between requests"""
        sleep_time = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)

    def confirm_agreement(self):
        """
        Access the agreement page and post the agreement to get the cookie.
        """
        try:
            # 1. First, access the agreement page to get cookies and CSRF tokens
            print(f"[STEP 1] Accessing agreement page: {self.agreement_url}")
            response_get = self.session.get(self.agreement_url, timeout=30)
            response_get.raise_for_status()
            print(f"[DEBUG] Initial GET status: {response_get.status_code}")
            print(f"[DEBUG] Cookies after initial GET: {self.session.cookies.get_dict()}")

            # 2. Extract CSRF tokens - REMOVED as tokens are not found in HTML
            # print("[STEP 2] Extracting CSRF tokens...")
            # soup = BeautifulSoup(response_get.content, 'html.parser')
            # struts_token_input = soup.find('input', {'name': 'org.apache.struts.taglib.html.TOKEN'})
            # csrf_token_input = soup.find('input', {'name': '_csrf'})
            # ... (token finding and error checking removed) ...
            print("[STEP 2] Skipping token extraction as tokens were not found in initial HTML.")

            # 3. Submit agreement form with "agree=on" to the correct action URL
            agreement_post_url = "https://hatsuden-kokai.occto.or.jp/hks-web-public/disclaimer-agree/next" # UPDATED URL
            print(f"[STEP 3] Submitting agreement POST to: {agreement_post_url}")
            agreement_data = {
                # No tokens found, just send agree=on
                "agree": "on"
            }
            print(f"[DEBUG] Posting data: {agreement_data}") 
            
            # Allow redirects, as successful POST might redirect
            response_post = self.session.post(agreement_post_url, data=agreement_data, timeout=30, allow_redirects=True) # Use updated URL
            print(f"[DEBUG] Agreement POST status: {response_post.status_code}")
            print(f"[DEBUG] Agreement POST final URL: {response_post.url}") # Check final URL after redirects
            print(f"[DEBUG] Cookies after agreement POST: {self.session.cookies.get_dict()}") # Check cookies again
            
            # Check if POST was likely successful
            response_post.raise_for_status() # Check for HTTP errors like 4xx, 5xx
            
            # It's better to check the final URL or content if possible, but for now assume success if POST OK
            print("Agreement POST request sent successfully.")
            self.agreement_confirmed = True
            return True
                
        except requests.RequestException as e:
            print(f"Error during agreement process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                # print(f"Response text: {e.response.text[:500]}") # Uncomment to debug response
            return False
        except Exception as e:
             print(f"Error parsing agreement page HTML or other unexpected error: {e}")
             return False

    def download_plant_operation_data(self, start_date: date, end_date: date):
        """
        Downloads power plant operation data as JSON for the specified date range.
        Returns the parsed JSON response on success, None otherwise.

        Args:
            start_date: Start date for the data (inclusive)
            end_date: End date for the data (inclusive)
        """
        # Ensure agreement is confirmed first
        if not self.agreement_confirmed and not self.confirm_agreement():
            print("Failed to confirm agreement. Cannot proceed with download.")
            return None # Return None on failure

        # Format dates for the search POST and download GET
        start_date_str_post = start_date.strftime("%Y/%m/%d")
        end_date_str_post = end_date.strftime("%Y/%m/%d")
        start_date_str_get = start_date.strftime("%Y%m%d") # Format for filename/URL params
        end_date_str_get = end_date.strftime("%Y%m%d")

        # -- Step 1: POST to the search endpoint --
        search_url = "https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks/search"
        print(f"[STEP 4] Posting search request to: {search_url}")

        # Define area and generation type codes (based on observed data)
        area_codes = ['99'] + [f"{i:02}" for i in range(1, 11)] # 99, 01..10
        hosiki_codes = ['99'] + [f"{i:02}" for i in range(1, 10)] # 99, 01..09

        # Prepare multipart/form-data payload using the 'files' parameter
        # Use tuples (None, value) for form fields
        search_payload = [
            ('htdnsCd', (None, '')),
            ('htdnsNm', (None, '')),
            ('unitNm', (None, '')),
            ('tgtDateDateFrom', (None, start_date_str_post)),
            ('tgtDateDateTo', (None, end_date_str_post))
        ]
        # Add multiple values for checkboxes
        search_payload.extend([('areaCheckbox', (None, code)) for code in area_codes])
        search_payload.extend([('hatudenHosikiCheckbox', (None, code)) for code in hosiki_codes])

        # Define necessary headers for the AJAX request
        search_headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks', # Referer from the page initiating the search
            # Content-Type with boundary will be set automatically by requests when using 'files'
        }

        print(f"[DEBUG] Search POST payload (first few items): {search_payload[:5]}...")
        print(f"[DEBUG] Search POST headers: {search_headers}")
        print(f"[DEBUG] Cookies before search POST: {self.session.cookies.get_dict()}")

        try:
            response_search = self.session.post(search_url, files=search_payload, headers=search_headers, timeout=30)
            print(f"[DEBUG] Search POST status: {response_search.status_code}")
            print(f"[DEBUG] Search POST Response Headers: {response_search.headers}")
            response_search.raise_for_status()

            # Check response content type
            search_content_type = response_search.headers.get('Content-Type', '').lower()
            print(f"[DEBUG] Search POST Response Content-Type: {search_content_type}")

            if 'application/json' not in search_content_type:
                print(f"[ERROR] Expected JSON response from search, but got {search_content_type}.")
                print(f"[DEBUG] Search Response Content (first 500 bytes): {response_search.content[:500]}")
                return None # Return None on failure

            # Parse the JSON response
            search_json_response = response_search.json()
            print("[DEBUG] Search POST JSON Response:")
            # print(json.dumps(search_json_response, indent=2, ensure_ascii=False)) # Pretty print JSON for debugging
            print(f"{str(search_json_response)[:500]}...") # Print start of JSON

            # TODO: Analyze search_json_response to determine the next step
            # - Does it contain the download URL?
            # - Does it contain a token needed for download?
            # - Does it contain the data directly (less likely for CSV)?
            print("[INFO] Search POST successful. JSON response received.")
            # REMOVED analysis print statement as it's now clear data is in JSON
            # print("[TODO] Need to analyze JSON to find CSV download method/URL.")

            # -- REMOVED Step 2: GET the CSV data (Placeholder) --
            # The data is directly in the JSON response from the search POST.
            # No separate download step is needed.

            # Generate filename for saving (moved here from removed block)
            # Commenting out the unused save_to_temp logic
            # if save_to_temp:
            #     start_date_filename = start_date_str_get
            #     end_date_filename = end_date_str_get
            #     filename = f"occto_plant_{start_date_filename}_to_{end_date_filename}.csv"
            #     save_path = os.path.join(self.temp_dir, filename)
            # else:
            #     save_path = None
            save_path = None # Explicitly set save_path to None as saving is not used

            # Process and display the data directly from the JSON response
            # self.process_and_display_data(search_json_response, start_date_str_post, end_date_str_post, max_rows, save_path)
            # The process_and_display_data function is no longer called here.
            # Data processing happens in main() after this function returns.

            # Return the parsed JSON data instead of processing/displaying here
            return search_json_response

        except requests.RequestException as e:
            # Handle errors for search POST
            print(f"Error during download process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text (first 500 bytes): {e.response.text[:500]}")
            return None # Return None on failure
        except Exception as e:
             print(f"An unexpected error occurred: {e}")
             import traceback
             traceback.print_exc() # Print full traceback for unexpected errors
             return None # Return None on failure

    def process_and_display_data(self, json_data, start_date_str, end_date_str, max_rows=10, save_path=None):
        """
        Process the JSON data into a DataFrame, display it, and optionally save it.

        Args:
            json_data (dict): The JSON response from the /search endpoint.
            start_date_str (str): The start date used for display/filename.
            end_date_str (str): The end date used for display/filename.
            max_rows (int): Maximum number of rows to display.
            save_path (str or None): Path to save the CSV file, or None.
        """
        try:
            if not json_data or 'data' not in json_data or 'items' not in json_data['data']:
                print("[ERROR] JSON response is missing expected 'data.items' structure.")
                return

            items = json_data['data']['items']
            if not items:
                print("[INFO] No data items found for the specified period.")
                return

            # Convert list of dictionaries to DataFrame
            df = pd.DataFrame(items)
            print(f"[INFO] Successfully parsed {len(df)} records from JSON response.")

            # Optional: Rename columns if needed (e.g., from Japanese to English)
            # column_mapping = { 'htdnsCd': 'plant_code', ... }
            # df.rename(columns=column_mapping, inplace=True)

            # Save the DataFrame to CSV if requested
            if save_path:
                try:
                    # Ensure the directory exists
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    # Save with appropriate encoding (UTF-8 is generally safe for wider compatibility)
                    df.to_csv(save_path, index=False, encoding='utf_8_sig') # Use utf_8_sig to include BOM for Excel
                    print(f"Data file saved to: {save_path}")
                except Exception as e_save:
                    print(f"[ERROR] Failed to save data to CSV: {e_save}")

            # Display the dataframe
            total_rows = len(df)
            print("\n" + "="*80)
            print(f"OCCTO Plant Operation Data - {start_date_str} to {end_date_str}")
            print(f"Total records: {total_rows}")
            print("="*80)
            # Displaying the first few rows as per max_rows
            print(df.head(max_rows).to_string())
            if total_rows > max_rows:
                print(f"... and {total_rows - max_rows} more rows.")

        except Exception as e:
            print(f"Error processing or displaying data: {e}")
            import traceback
            traceback.print_exc()

    def _process_json_to_dataframe(self, json_data):
        """
        Process the JSON data from the /search endpoint into a Pandas DataFrame.

        Args:
            json_data (dict): The JSON response from the /search endpoint.

        Returns:
            pd.DataFrame: A DataFrame containing the raw data from JSON, or empty if error.
        """
        try:
            if not json_data or 'data' not in json_data or 'items' not in json_data['data']:
                logger.error("[ERROR] JSON response is missing expected 'data.items' structure.")
                return pd.DataFrame()

            items = json_data['data']['items']
            if not items:
                logger.info("[INFO] No data items found in the JSON response.")
                return pd.DataFrame()

            # Convert list of dictionaries to DataFrame
            df = pd.DataFrame(items)
            logger.info(f"[INFO] Successfully parsed {len(df)} records from JSON response into DataFrame.")
            return df

        except Exception as e:
            logger.error(f"Error processing JSON data to DataFrame: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame() # Return empty DataFrame on error

def parse_args():
    parser = argparse.ArgumentParser(description="Download OCCTO 30-min generation data and import to DB") # Updated description
    parser.add_argument(
        "--start-date", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help="Start date (YYYY-MM-DD)",
        required=True
    )
    parser.add_argument(
        "--end-date", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help="End date (YYYY-MM-DD)",
        default=None
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to the SQLite database file (defaults to ./power_market_data.db)"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    # If end_date is not specified, use the start_date
    end_date = args.end_date if args.end_date else args.start_date
    
    logger.info(f"Starting OCCTO data download for {args.start_date} to {end_date}...")

    # Create downloader instance
    downloader = OCCTOStandaloneDownloader() # Class name might still be Standalone
    
    # Get raw JSON data
    json_data = downloader.download_plant_operation_data(
        start_date=args.start_date,
        end_date=end_date,
        # Removed max_rows and save_to_temp arguments as they are no longer used here
    ) # This method needs to be modified to return the JSON or DataFrame

    if not json_data:
         logger.error("Failed to download data. Exiting.")
         return

    # Process JSON to DataFrame (using the new internal method)
    raw_df = downloader._process_json_to_dataframe(json_data)

    if raw_df.empty:
        logger.warning("No data obtained after processing JSON. Exiting.")
        return
        
    logger.info("Data download and initial processing complete.")
    logger.info("Initializing database importer...")

    # Import data into database
    try:
        importer = OCCTO30MinDBImporter(db_path=args.db_path)
        transformed_df = importer.transform_occto_data(raw_df)
        
        if not transformed_df.empty:
            logger.info("Inserting data into the database...")
            importer.insert_occto_data(transformed_df)
            logger.info("Database import process finished.")
        else:
             logger.warning("Data transformation resulted in an empty DataFrame. Nothing inserted.")

    except Exception as e:
        logger.error(f"An error occurred during database import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 