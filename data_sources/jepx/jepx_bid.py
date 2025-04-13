#!/usr/bin/env python3
import requests
from datetime import datetime, timedelta
import sys
import time
import random
import os
from typing import Optional
from db import DuckDBConnection

class JEPXBidDownloader:
    """JEPX bid data downloader class."""
    
    BASE_URL = "https://www.jepx.jp/js/csv_read.php"
    DIR_NAMES = ["spot_bid_curves", "spot_splitting_areas"]
    
    def __init__(self):
        """Initialize the downloader with database connection."""
        self.db = DuckDBConnection()
    
    def download_csv(self, date: datetime, dir_name: str) -> Optional[bytes]:
        """
        Download CSV data for a specific date and directory.
        
        Args:
            date (datetime): The target date
            dir_name (str): The directory name on JEPX server
            
        Returns:
            Optional[bytes]: The CSV content if successful, None otherwise
        """
        date_str = date.strftime("%Y%m%d")
        params = {
            "dir": dir_name,
            "file": f"{dir_name}_{date_str}.csv"
        }
        
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ja',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'www.jepx.jp',
            'If-Modified-Since': 'Thu, 01 Jun 1970 00:00:00 GMT',
            'Pragma': 'no-cache',
            'Referer': 'https://www.jepx.jp/electricpower/market-data/spot/bid_curves.html',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15'
        }
        
        try:
            with requests.Session() as session:
                response = session.get(self.BASE_URL, params=params, headers=headers)
                if response.status_code == 200:
                    print(f"Downloaded: {dir_name}_{date_str}.csv")
                    return response.content
                else:
                    print(f"Failed to download {dir_name}_{date_str}.csv: Status code {response.status_code}")
                    return None
        except Exception as e:
            print(f"Error downloading {dir_name}_{date_str}.csv: {e}")
            return None

    def download_files(self, start_date: datetime, end_date: datetime) -> None:
        """
        Download files for a date range.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        """
        current_date = start_date
        while current_date <= end_date:
            for dir_name in self.DIR_NAMES:
                content = self.download_csv(current_date, dir_name)
                if content is not None:
                    # Save to database
                    self._save_to_db(dir_name, current_date, content)
            sleep_time = random.randint(10, 20)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            current_date += timedelta(days=1)

    def _save_to_db(self, dir_name: str, date: datetime, content: bytes) -> None:
        """
        Save downloaded content to database.
        
        Args:
            dir_name (str): The directory name
            date (datetime): The target date
            content (bytes): The CSV content
        """
        cursor = self.db.get_connection().cursor()
        cursor.execute(
            "INSERT INTO downloaded_csv (dir_name, date, content) VALUES (?, ?, ?)",
            (dir_name, date.strftime("%Y%m%d"), content)
        )
        self.db.get_connection().commit()

def main():
    """Main entry point for the JEPX bid data downloader."""
    start_date_str = input("Enter start date (YYYY-MM-DD): ")
    end_date_str = input("Enter end date (YYYY-MM-DD): ")
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    
    downloader = JEPXBidDownloader()
    downloader.download_files(start_date, end_date)
    print("Download completed.")

if __name__ == "__main__":
    main()
