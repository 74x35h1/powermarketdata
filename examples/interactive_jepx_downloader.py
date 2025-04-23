#!/usr/bin/env python3
"""
Interactive JEPX Data Downloader CLI

This script provides a command-line interface for downloading JEPX (Japan Electric Power Exchange) data
for a specified date range. The downloaded data is displayed and optionally saved to a database.
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import time
import random
import json
from typing import Optional, List, Dict, Any, Tuple

# Add the parent directory to sys.path to allow imports from data_sources
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources.jepx.jepx_bid import JEPXBidDownloader
from data_sources.jepx.jepx_da_price import JEPXDAPriceDownloader
from db.duckdb_connection import DuckDBConnection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

JEPX_DATA_TYPES = {
    "1": {"name": "約定結果", "class": JEPXBidDownloader, "table": "jepx_bid_data"},
    "2": {"name": "スポット価格", "class": JEPXDAPriceDownloader, "table": "jepx_da_price"},
    # Add other JEPX data types as they are implemented
}

def print_header() -> None:
    """Display the application header and instructions."""
    print("\n" + "=" * 60)
    print("JEPX Data Downloader".center(60))
    print("=" * 60)
    print("\nこのツールは、JEPXからデータをダウンロードし、表示します。")
    print("This tool downloads and displays data from JEPX (Japan Electric Power Exchange).")
    print("\n" + "-" * 60)

def display_data_type_choices() -> None:
    """Display available JEPX data type choices."""
    print("\nSelect JEPX data type:")
    for key, value in JEPX_DATA_TYPES.items():
        print(f"{key}: {value['name']}")
    print("q: Quit")

def get_user_data_type_selection() -> str:
    """Get user selection for JEPX data type."""
    while True:
        selection = input("\nEnter your choice (1-{} or q to quit): ".format(len(JEPX_DATA_TYPES)))
        if selection.lower() == 'q':
            sys.exit(0)
        if selection in JEPX_DATA_TYPES:
            return selection
        print("Invalid selection. Please try again.")

def get_date_selection() -> Tuple[datetime, datetime]:
    """Get date selection from user."""
    # Default to last month if no input
    default_end_date = datetime.now().replace(day=1) - timedelta(days=1)
    default_start_date = default_end_date.replace(day=1)
    
    # Format default dates for display
    default_start_str = default_start_date.strftime("%Y-%m-%d")
    default_end_str = default_end_date.strftime("%Y-%m-%d")
    
    print(f"\nDefault date range: {default_start_str} to {default_end_str}")
    
    while True:
        start_date_str = input(f"Enter start date (YYYY-MM-DD) [{default_start_str}]: ")
        end_date_str = input(f"Enter end date (YYYY-MM-DD) [{default_end_str}]: ")
        
        # Use defaults if input is empty
        if not start_date_str.strip():
            start_date_str = default_start_str
        if not end_date_str.strip():
            end_date_str = default_end_str
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            if start_date > end_date:
                print("Error: Start date must be before or equal to end date.")
                continue
                
            return start_date, end_date
        except ValueError:
            print("Error: Dates must be in YYYY-MM-DD format.")

def display_bid_data(data_type_id: str, start_date: datetime, end_date: datetime) -> None:
    """Display JEPX bid data from the database."""
    data_type = JEPX_DATA_TYPES[data_type_id]
    table_name = data_type["table"]
    
    try:
        db = DuckDBConnection()
        
        # Query to fetch data for the specified date range
        query = f"""
            SELECT date, slot, area_code, bid 
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
            ORDER BY date, slot, area_code
            LIMIT 10
        """
        
        results = db.execute_query(
            query, 
            (start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
        )
        
        if not results:
            print("\nNo data found for the specified date range.")
            return
            
        print(f"\nShowing first 10 records from {table_name} for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}:")
        
        # Create a formatted display of the data
        rows = []
        for row in results:
            date, slot, area_code, bid_json = row
            bid_data = json.loads(bid_json) if isinstance(bid_json, str) else bid_json
            # Take just the first bid point for display
            first_bid = bid_data[0] if bid_data else {}
            
            rows.append([
                date,
                slot,
                area_code,
                first_bid.get("price", "N/A"),
                first_bid.get("sell_qty", "N/A"),
                first_bid.get("buy_qty", "N/A"),
                f"{len(bid_data)} points"
            ])
        
        headers = ["Date", "Slot", "Area Code", "Price", "Sell Qty", "Buy Qty", "Total Points"]
        print(tabulate(rows, headers=headers, tablefmt="pretty"))
        
        # Show how many records total
        count_query = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE date BETWEEN ? AND ?
        """
        count = db.execute_query(
            count_query, 
            (start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
        )
        total_count = count[0][0] if count else 0
        
        print(f"\nTotal records in database for this period: {total_count}")
        
    except Exception as e:
        logger.error(f"Error displaying data: {str(e)}")
        print(f"Error: {str(e)}")

def display_price_data(data_type_id: str, start_date: datetime, end_date: datetime) -> None:
    """Display JEPX price data from the database."""
    data_type = JEPX_DATA_TYPES[data_type_id]
    table_name = data_type["table"]
    
    try:
        db = DuckDBConnection()
        
        # Query to fetch data for the specified date range
        query = f"""
            SELECT date, slot, ap0_system, ap3_tokyo, ap6_kansai, contract_qty_kwh
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
            ORDER BY date, slot
            LIMIT 10
        """
        
        results = db.execute_query(
            query, 
            (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        )
        
        if not results:
            print("\nNo data found for the specified date range.")
            return
            
        print(f"\nShowing first 10 records from {table_name} for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}:")
        
        # Create a formatted display of the data
        rows = []
        for row in results:
            date, slot, system_price, tokyo_price, kansai_price, contract_qty = row
            rows.append([
                date,
                slot,
                f"{system_price:.2f}" if system_price is not None else "N/A",
                f"{tokyo_price:.2f}" if tokyo_price is not None else "N/A",
                f"{kansai_price:.2f}" if kansai_price is not None else "N/A",
                f"{contract_qty:,}" if contract_qty is not None else "N/A"
            ])
        
        headers = ["Date", "Slot", "System Price", "Tokyo Price", "Kansai Price", "Contract Qty (kWh)"]
        print(tabulate(rows, headers=headers, tablefmt="pretty"))
        
        # Show how many records total
        count_query = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE date BETWEEN ? AND ?
        """
        count = db.execute_query(
            count_query, 
            (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        )
        total_count = count[0][0] if count else 0
        
        print(f"\nTotal records in database for this period: {total_count}")
        
    except Exception as e:
        logger.error(f"Error displaying data: {str(e)}")
        print(f"Error: {str(e)}")

def display_data(data_type_id: str, start_date: datetime, end_date: datetime) -> None:
    """Display downloaded data from the database based on data type."""
    if data_type_id == "1":  # 約定結果
        display_bid_data(data_type_id, start_date, end_date)
    elif data_type_id == "2":  # スポット価格
        display_price_data(data_type_id, start_date, end_date)
    else:
        print(f"Display not implemented for data type: {JEPX_DATA_TYPES[data_type_id]['name']}")

def download_and_display_data(data_type_id: str, start_date: datetime, end_date: datetime) -> None:
    """Download and display data based on user selection."""
    data_type = JEPX_DATA_TYPES[data_type_id]
    print(f"\nDownloading {data_type['name']} data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    
    try:
        # Initialize the appropriate downloader
        downloader = data_type['class']()
        
        # Download data
        if data_type_id == "1":  # 約定結果
            downloader.download_and_save(start_date, end_date)
        elif data_type_id == "2":  # スポット価格
            # Use the first day of the month of start_date and 
            # the last day of the month of end_date to fetch all relevant data
            first_month = start_date.replace(day=1)
            last_month = end_date.replace(day=1)
            # Always fetch the most recent data
            downloader.fetch_and_store(first_month, last_month)
        else:
            print(f"Download not implemented for data type: {data_type['name']}")
            return
        
        print("\nDownload completed successfully!")
        
        # Display the data that was just downloaded
        display_data(data_type_id, start_date, end_date)
        
    except Exception as e:
        logger.error(f"Error downloading data: {str(e)}")
        print(f"Error: {str(e)}")

def main() -> None:
    """Main function to orchestrate the CLI flow."""
    try:
        print_header()
        
        while True:
            display_data_type_choices()
            data_type_id = get_user_data_type_selection()
            start_date, end_date = get_date_selection()
            
            download_and_display_data(data_type_id, start_date, end_date)
            
            # Ask user if they want to continue
            continue_choice = input("\nDo you want to download more data? (y/n): ")
            if continue_choice.lower() != 'y':
                break
        
        print("\nThank you for using the JEPX Data Downloader. Goodbye!")
    
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Exiting...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\nAn unexpected error occurred: {str(e)}")
        print("Please check the logs for more information.")
    
    sys.exit(0)

if __name__ == "__main__":
    main() 