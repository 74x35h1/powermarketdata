"""
TSO (Transmission System Operator) demand data downloader.

This module provides functionality for downloading demand data from 
Japanese Transmission System Operators.
"""

import os
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Union, Any
import sys

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.tso.tso_urls import get_tso_url, get_area_code, TSO_INFO
from data_sources.db_connection import DuckDBConnection

logger = logging.getLogger(__name__)

class TSODemandDownloader:
    """Class for downloading demand data from TSOs."""
    
    def __init__(self, db_path: str = "powermarket.duckdb"):
        """
        Initialize the TSO demand downloader.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db = DuckDBConnection(db_path)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def download_demand_data(self, tso_id: str, year: int, month: int) -> Optional[pd.DataFrame]:
        """
        Download demand data for a specific TSO and time period.
        
        Args:
            tso_id: The ID of the TSO (e.g., 'tepco', 'kepco')
            year: The year for which to get data
            month: The month for which to get data (1-12)
            
        Returns:
            DataFrame containing the demand data or None if download failed
        """
        url = get_tso_url(tso_id, year, month)
        if not url:
            logger.error(f"Invalid TSO ID: {tso_id}")
            return None
            
        area_code = get_area_code(tso_id)
        if not area_code:
            logger.error(f"Could not find area code for TSO: {tso_id}")
            return None
            
        logger.info(f"Downloading {tso_id} demand data for {year}-{month:02d}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Some TSOs may use different encodings
            encodings = ['utf-8', 'shift-jis', 'cp932', 'euc-jp']
            data = None
            
            for encoding in encodings:
                try:
                    data = pd.read_csv(
                        pd.io.common.StringIO(response.content.decode(encoding)),
                        encoding=encoding
                    )
                    break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            
            if data is None:
                logger.error(f"Failed to decode CSV data from {tso_id}")
                return None
                
            # Process and standardize the data
            processed_data = self._process_data(data, tso_id, area_code, year, month)
            
            if processed_data is not None and not processed_data.empty:
                # Save to database
                self._save_to_db(processed_data, tso_id)
                return processed_data
                
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download data from {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing {tso_id} data: {str(e)}")
            return None
    
    def _process_data(self, data: pd.DataFrame, tso_id: str, area_code: str, 
                      year: int, month: int) -> Optional[pd.DataFrame]:
        """
        Process and standardize the downloaded data.
        
        Args:
            data: The raw DataFrame from the TSO
            tso_id: The ID of the TSO
            area_code: The area code for the TSO
            year: The year of the data
            month: The month of the data
            
        Returns:
            Standardized DataFrame with demand data
        """
        # This method would need to be customized for each TSO's data format
        # as they all have different column names and structures
        try:
            # Log the first few rows to understand the structure
            logger.debug(f"Data columns: {data.columns.tolist()}")
            logger.debug(f"First row: {data.iloc[0].to_dict()}")
            
            # TODO: Implement TSO-specific data processing
            # This is a placeholder implementation and would need to be expanded
            
            # Create a standardized DataFrame
            std_data = pd.DataFrame({
                'date': [],
                'hour': [],
                'area_code': [],
                'demand_mwh': [],
                'tso_id': []
            })
            
            # Return the processed data
            return std_data
            
        except Exception as e:
            logger.error(f"Error processing data for {tso_id}: {str(e)}")
            return None
    
    def _save_to_db(self, data: pd.DataFrame, tso_id: str) -> bool:
        """
        Save the processed data to the database.
        
        Args:
            data: The processed DataFrame to save
            tso_id: The ID of the TSO
            
        Returns:
            True if successful, False otherwise
        """
        try:
            table_name = f"tso_demand_{tso_id}"
            
            # Create table if it doesn't exist
            self.db.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date DATE,
                    hour INTEGER,
                    area_code VARCHAR,
                    demand_mwh DOUBLE,
                    tso_id VARCHAR,
                    PRIMARY KEY (date, hour, area_code)
                )
            """)
            
            # Insert data
            self.db.insert_df(table_name, data)
            logger.info(f"Saved {len(data)} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data to database: {str(e)}")
            return False
    
    def download_range(self, tso_id: str, start_date: datetime, end_date: datetime) -> List[pd.DataFrame]:
        """
        Download demand data for a specified date range.
        
        Args:
            tso_id: The ID of the TSO
            start_date: The start date (inclusive)
            end_date: The end date (inclusive)
            
        Returns:
            List of DataFrames containing the demand data
        """
        results = []
        
        current_date = start_date
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            
            df = self.download_demand_data(tso_id, year, month)
            if df is not None and not df.empty:
                results.append(df)
            
            # Get next month
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            
            current_date = datetime(year, month, 1)
            
            # Add a random delay to avoid hammering the server
            time.sleep(random.uniform(1.0, 3.0))
        
        return results
    
    def download_all_tsos(self, year: int, month: int) -> Dict[str, pd.DataFrame]:
        """
        Download demand data for all TSOs for a specific month.
        
        Args:
            year: The year for which to get data
            month: The month for which to get data (1-12)
            
        Returns:
            Dictionary mapping TSO IDs to their respective demand DataFrames
        """
        results = {}
        
        for tso_id in TSO_INFO.keys():
            df = self.download_demand_data(tso_id, year, month)
            if df is not None and not df.empty:
                results[tso_id] = df
            
            # Add a random delay to avoid hammering the server
            time.sleep(random.uniform(2.0, 5.0))
        
        return results


def main():
    """Main function to demonstrate TSO data downloading."""
    # Create a downloader instance
    downloader = TSODemandDownloader()
    
    # Example: Download data for TEPCO for the current month
    now = datetime.now()
    year = now.year
    month = now.month
    
    # Try downloading from TEPCO as an example
    tso_id = "tepco"
    print(f"\nDownloading data for {tso_id} ({TSO_INFO[tso_id]['name']}) - {year}/{month:02d}")
    data = downloader.download_demand_data(tso_id, year, month)
    
    if data is not None and not data.empty:
        print(f"\nSuccessfully downloaded data for {tso_id}")
        print(f"Sample data (first {len(data)} entries):")
        print(data.head())
    else:
        print(f"\nFailed to download data for {tso_id}")
    
    # Optionally try another TSO for comparison
    tso_id = "kepco"
    print(f"\nDownloading data for {tso_id} ({TSO_INFO[tso_id]['name']}) - {year}/{month:02d}")
    data = downloader.download_demand_data(tso_id, year, month)
    
    if data is not None and not data.empty:
        print(f"\nSuccessfully downloaded data for {tso_id}")
        print(f"Sample data (first {len(data)} entries):")
        print(data.head())
    else:
        print(f"\nFailed to download data for {tso_id}")


if __name__ == "__main__":
    main() 