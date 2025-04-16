#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Explicitly import modules
from db.duckdb_connection import DuckDBConnection
from data_sources.jepx.jepx_bid import JEPXBidDownloader

def test_jepx_download_and_save():
    """Test downloading JEPX data and saving it to the database."""
    # Create a JEPXBidDownloader instance
    downloader = JEPXBidDownloader()
    
    # Use a single day for testing
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    print(f"Testing download and save for date: {yesterday.strftime('%Y-%m-%d')}")
    
    # Download and save data for yesterday
    downloader.download_and_save(yesterday, yesterday)
    
    # Check if data was saved to the database
    conn = downloader.db.get_connection()
    
    # Check the jepx_bid_data table
    result = conn.execute("SELECT COUNT(*) FROM jepx_bid_data").fetchone()
    print(f"Number of records in jepx_bid_data: {result[0]}")
    
    # Check the downloaded_files table
    result = conn.execute("SELECT COUNT(*) FROM downloaded_files").fetchone()
    print(f"Number of records in downloaded_files: {result[0]}")
    
    # Display some sample data if available
    if result[0] > 0:
        print("\nSample downloaded files:")
        files = conn.execute("""
            SELECT dir_name, date, filename, status
            FROM downloaded_files
            LIMIT 5
        """).fetchall()
        for file in files:
            print(f"  {file[0]} - {file[1]} - {file[2]} - {file[3]}")
        
        print("\nSample JEPX data:")
        data = conn.execute("""
            SELECT date, time_slot, area_seq
            FROM jepx_bid_data
            LIMIT 5
        """).fetchall()
        for item in data:
            print(f"  Date: {item[0]}, Time Slot: {item[1]}, Area: {item[2]}")
    
    print("\nTest completed.")

if __name__ == "__main__":
    test_jepx_download_and_save() 