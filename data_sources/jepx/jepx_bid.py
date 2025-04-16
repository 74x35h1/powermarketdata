#!/usr/bin/env python3
import requests
from datetime import datetime, timedelta
import sys
import time
import random
import csv
import json
import os
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path

# Add the project root to the Python path to ensure proper imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from db.duckdb_connection import DuckDBConnection

class JEPXBidDownloader:
    """JEPX bid data downloader class."""
    
    BASE_URL = "https://www.jepx.jp/js/csv_read.php"
    DIR_NAMES = ["spot_bid_curves", "spot_splitting_areas"]
    
    def __init__(self):
        """Initialize the downloader with database connection."""
        self.db = DuckDBConnection()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Ensure necessary tables exist in the database."""
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS jepx_bid_data (
                id TEXT PRIMARY KEY,
                date TEXT,
                slot INTEGER,
                area_code INTEGER,
                bid TEXT
            )
        """)
        
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS downloaded_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_name VARCHAR,
                date DATE,
                filename VARCHAR,
                status VARCHAR,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
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
                    self._record_download(dir_name, date, f"{dir_name}_{date_str}.csv", "success")
                    return response.content
                else:
                    print(f"Failed to download {dir_name}_{date_str}.csv: Status code {response.status_code}")
                    self._record_download(dir_name, date, f"{dir_name}_{date_str}.csv", f"failed: {response.status_code}")
                    return None
        except Exception as e:
            print(f"Error downloading {dir_name}_{date_str}.csv: {e}")
            self._record_download(dir_name, date, f"{dir_name}_{date_str}.csv", f"error: {str(e)}")
            return None
    
    def _record_download(self, dir_name: str, date: datetime, filename: str, status: str):
        """Record download attempt in the database."""
        query = """
            INSERT INTO downloaded_files (dir_name, date, filename, status)
            VALUES (?, ?, ?, ?)
        """
        self.db.execute_query(query, (dir_name, date.strftime("%Y-%m-%d"), filename, status))
    
    def process_csv_to_json(self, csv_text: str):
        data = {}
        reader = csv.reader(csv_text.splitlines())
        for row in reader:
            if len(row) < 6:
                continue
            date, slot, price, sell_qty, buy_qty, area_code = row[0], row[1], row[2], row[3], row[4], row[5]
            # 型変換とバリデーション
            try:
                slot_int = int(slot)
                area_code_int = int(area_code)
            except ValueError:
                continue
            key = (date, slot_int, area_code_int)
            order = {"price": price, "sell_qty": sell_qty, "buy_qty": buy_qty}
            if key not in data:
                data[key] = []
            data[key].append(order)
        records = []
        for (date, slot, area_code), bids in data.items():
            record = {
                "id": f"{date}_{slot}_{area_code}",
                "date": date,
                "slot": slot,
                "area_code": area_code,
                "bid": bids
            }
            records.append(record)
        return records
    
    def save_to_database(self, records):
        for record in records:
            try:
                # デバッグ出力
                print(f"Saving record: {record}")
                query = """
                    INSERT INTO jepx_bid_data (id, date, slot, area_code, bid)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET bid=excluded.bid
                """
                self.db.execute_query(
                    query,
                    (
                        str(record["id"]),
                        str(record["date"]),
                        int(record["slot"]),
                        int(record["area_code"]),
                        json.dumps(record["bid"])
                    )
                )
            except Exception as e:
                print(f"Error saving record to database: {e}")
    
    def download_and_save(self, start_date: datetime, end_date: datetime) -> None:
        """
        Download files for a date range and save to database.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        """
        current_date = start_date
        while current_date <= end_date:
            for dir_name in self.DIR_NAMES:
                content = self.download_csv(current_date, dir_name)
                if content is not None:
                    try:
                        text = content.decode('utf-8')
                    except UnicodeDecodeError:
                        text = content.decode('shift_jis', errors='replace')
                    records = self.process_csv_to_json(text)
                    self.save_to_database(records)
                    print(f"Saved {dir_name} data for {current_date.strftime('%Y-%m-%d')} to database")
            
            sleep_time = random.randint(10, 20)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            current_date += timedelta(days=1)

def download_csv(date: datetime, dir_name: str) -> Optional[bytes]:
    """
    Standalone function to download CSV data for a specific date and directory.
    
    Args:
        date (datetime): The target date
        dir_name (str): The directory name on JEPX server
        
    Returns:
        Optional[bytes]: The CSV content if successful, None otherwise
    """
    downloader = JEPXBidDownloader()
    return downloader.download_csv(date, dir_name)

def main():
    """Main entry point for the JEPX bid data downloader."""
    print("JEPX Bid Data Downloader")
    print("========================")
    
    start_date_str = input("Enter start date (YYYY-MM-DD): ")
    end_date_str = input("Enter end date (YYYY-MM-DD): ")
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    
    downloader = JEPXBidDownloader()
    downloader.download_and_save(start_date, end_date)
    print("Download and database insertion completed.")

if __name__ == "__main__":
    main()
