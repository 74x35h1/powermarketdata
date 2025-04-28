#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCCTO Data Downloader

This module provides functionality to download and parse data from OCCTO 
(Organization for Cross-regional Coordination of Transmission Operators, Japan).
"""

import os
import logging
import requests
import pandas as pd
import time
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union, Any

logger = logging.getLogger(__name__)

class OCCTODownloader:
    """
    Class for downloading and parsing data from OCCTO
    (Organization for Cross-regional Coordination of Transmission Operators, Japan).
    """
    
    def __init__(self, base_url: str = "https://www.occto.or.jp/"):
        """
        Initialize the OCCTO downloader with the base URL.
        
        Args:
            base_url: Base URL for OCCTO website (default: "https://www.occto.or.jp/")
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        })
        # Create a directory for temporary data storage
        self.temp_dir = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "temp"))
        self.temp_dir.mkdir(exist_ok=True)
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close session."""
        self.session.close()

    def _random_sleep(self, min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
        """
        Sleep for a random amount of time between requests to avoid overloading the server.
        
        Args:
            min_seconds: Minimum sleep time in seconds
            max_seconds: Maximum sleep time in seconds
        """
        sleep_time = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)

    def download_file(self, url: str, save_path: Optional[Path] = None) -> Optional[bytes]:
        """
        Download a file from a given URL.
        
        Args:
            url: URL to download the file from
            save_path: Optional path to save the downloaded file
            
        Returns:
            Content of the downloaded file as bytes, or None if download failed
        """
        try:
            logger.info(f"Downloading file from {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            content = response.content
            
            if save_path:
                logger.info(f"Saving downloaded file to {save_path}")
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(content)
            
            return content
        
        except requests.RequestException as e:
            logger.error(f"Error downloading file from {url}: {e}")
            return None

    def get_plant_operation_url(self, target_date: date) -> str:
        """
        Generate the URL for power plant operation data for a specific date.
        
        Args:
            target_date: The date for which to generate the URL
            
        Returns:
            URL string for the power plant operation data
        """
        # Format: https://www.occto.or.jp/occto/performancedata/plantoperationperformance/YYYYMM_plantoperations.csv
        year_month = target_date.strftime("%Y%m")
        return f"{self.base_url.rstrip('/')}/occto/performancedata/plantoperationperformance/{year_month}_plantoperations.csv"

    def download_plant_operation_data(self, 
                                     target_date: date,
                                     max_rows: int = 10,
                                     save_to_temp: bool = True) -> None:
        """
        Download and display power plant operation data for the specified month.
        
        Args:
            target_date: Target date (only year and month are used)
            max_rows: Maximum number of rows to display
            save_to_temp: Whether to save downloaded files to temporary directory
        """
        url = self.get_plant_operation_url(target_date)
        
        if save_to_temp:
            filename = f"occto_plant_{target_date.strftime('%Y%m')}.csv"
            save_path = self.temp_dir / filename
        else:
            save_path = None
        
        print(f"Attempting to download plant operation data from OCCTO for {target_date.strftime('%Y-%m')}...")
        print(f"URL: {url}")
        
        content = self.download_file(url, save_path)
        
        if content:
            try:
                # Display the CSV data
                self.display_plant_operation_data(content, target_date, max_rows)
                print(f"Data file saved to: {save_path}" if save_to_temp else "")
            except Exception as e:
                print(f"Error processing data: {e}")
        else:
            print(f"Failed to download data from {url}")

    def display_plant_operation_data(self, content: bytes, target_date: date, max_rows: int = 10) -> None:
        """
        Display the downloaded plant operation data CSV.
        
        Args:
            content: CSV file content as bytes
            target_date: The target date for the data
            max_rows: Maximum number of rows to display
        """
        try:
            # Try to decode with UTF-8 first
            try:
                df = pd.read_csv(pd.io.common.BytesIO(content), encoding='utf-8')
            except UnicodeDecodeError:
                # If UTF-8 fails, try Shift-JIS (commonly used in Japanese systems)
                df = pd.read_csv(pd.io.common.BytesIO(content), encoding='shift-jis')
            
            # Check if DataFrame is empty or malformed
            if df.empty:
                print(f"No data found for {target_date.strftime('%Y-%m')}")
                return
            
            # Print data summary
            total_rows = len(df)
            
            print("\n" + "="*80)
            print(f"OCCTO Plant Operation Data - {target_date.strftime('%Y-%m')}")
            print(f"Total records: {total_rows}")
            print("="*80)
            
            # Print column names
            print("\nColumn Names:")
            for i, col in enumerate(df.columns):
                print(f"{i+1}. {col}")
            
            # Display sample rows
            rows_to_display = min(max_rows, total_rows)
            print(f"\nShowing {rows_to_display} of {total_rows} rows:")
            print("-"*80)
            
            # Display the first few rows
            pd.set_option('display.max_columns', None)  # Show all columns
            pd.set_option('display.width', 1000)  # Set display width
            pd.set_option('display.max_colwidth', 30)  # Limit column width
            
            if rows_to_display > 0:
                print(df.head(rows_to_display).to_string())
            
            # Count plant types if available
            plant_type_col = None
            for col_name in df.columns:
                if '発電方式' in col_name or '燃種' in col_name:
                    plant_type_col = col_name
                    break
            
            if plant_type_col and plant_type_col in df.columns:
                print("\nPlant Types Summary:")
                plant_types = df[plant_type_col].value_counts()
                for plant_type, count in plant_types.items():
                    print(f"- {plant_type}: {count} records")
            
            # Count areas if available
            area_col = None
            for col_name in df.columns:
                if 'エリア' in col_name or 'area' in col_name:
                    area_col = col_name
                    break
                    
            if area_col and area_col in df.columns:
                print("\nArea Summary:")
                areas = df[area_col].value_counts()
                for area, count in areas.items():
                    print(f"- {area}: {count} records")
            
            print("="*80)
            
        except Exception as e:
            print(f"Error displaying plant operation data: {e}")

    def parse_plant_operation_data(self, content: bytes, target_date: date) -> pd.DataFrame:
        """
        Parse the downloaded plant operation data CSV.
        
        Args:
            content: CSV file content as bytes
            target_date: The target date for the data
            
        Returns:
            Pandas DataFrame containing the parsed data
        """
        try:
            # Try to decode with UTF-8 first
            try:
                df = pd.read_csv(pd.io.common.BytesIO(content), encoding='utf-8')
            except UnicodeDecodeError:
                # If UTF-8 fails, try Shift-JIS (commonly used in Japanese systems)
                df = pd.read_csv(pd.io.common.BytesIO(content), encoding='shift-jis')
            
            # Check if DataFrame is empty or malformed
            if df.empty:
                logger.warning(f"Empty DataFrame for {target_date.strftime('%Y-%m')}")
                return pd.DataFrame()
            
            # Expected column format based on example
            expected_columns = {
                '発電所コード': 'plant_code',
                'エリア': 'area',
                '発電所名': 'plant_name',
                'ユニット名': 'unit_name',
                '発電方式・燃種': 'plant_type',
                '対象日': 'date'
                # Hourly data columns will be processed separately
            }
            
            # Extract time-based columns (00:30[kWh], 01:00[kWh], etc.)
            time_cols = [col for col in df.columns if '[kWh]' in col]
            
            # Map known columns
            rename_cols = {}
            for jp_col, en_col in expected_columns.items():
                if jp_col in df.columns:
                    rename_cols[jp_col] = en_col
            
            # Apply column mapping
            if rename_cols:
                df = df.rename(columns=rename_cols)
            
            # Convert date format if available
            date_col = 'date' if 'date' in df.columns else '対象日'
            if date_col in df.columns:
                try:
                    # Attempt to convert date string (e.g., '2025/04/01') to datetime
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                except Exception as e:
                    logger.warning(f"Failed to convert date column: {e}")
            
            # Add processing date
            df['processing_date'] = target_date.strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing plant operation data: {e}")
            return pd.DataFrame() 