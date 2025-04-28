#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCCTO Database Importer

This module provides functionality to import OCCTO data into the database.
"""

import os
import logging
import pandas as pd
from datetime import date, datetime
from typing import List, Tuple, Optional, Dict, Any, Union
from pathlib import Path

from db.duckdb_connection import DuckDBConnection
from data_sources.occto.occto_downloader import OCCTODownloader

logger = logging.getLogger(__name__)

class OCCTODataImporter:
    """
    Class for importing OCCTO data into the database.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the OCCTO data importer.
        
        Args:
            db_path: Path to the database file. If None, uses the default.
        """
        self.db_path = db_path
        self.db = None
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the database connection."""
        pass  # Using with context for DB operations

    def import_plant_operation_data(self, 
                                  start_date: date, 
                                  end_date: date,
                                  save_to_db: bool = True) -> int:
        """
        Import power plant operation data for the specified period.
        
        Args:
            start_date: Start date for data import
            end_date: End date for data import
            save_to_db: Whether to save the data to the database
            
        Returns:
            Number of rows imported
        """
        total_rows = 0
        
        try:
            # Download data
            with OCCTODownloader() as downloader:
                results = downloader.download_plant_operation_data(
                    start_date=start_date,
                    end_date=end_date,
                    save_to_temp=True
                )
            
            if not results:
                logger.warning("No data downloaded")
                return 0
            
            # Process and save to database
            all_dfs = []
            for target_date, df in results:
                if not df.empty:
                    all_dfs.append(df)
                    logger.info(f"Processed data for {target_date.strftime('%Y-%m')}: {len(df)} rows")
            
            if not all_dfs:
                logger.warning("No valid data to import")
                return 0
            
            # Combine all data
            combined_df = pd.concat(all_dfs, ignore_index=True)
            total_rows = len(combined_df)
            
            # Save to database if requested
            if save_to_db:
                self._save_to_db(combined_df)
                logger.info(f"Saved {total_rows} rows to database")
            
            return total_rows
        
        except Exception as e:
            logger.error(f"Error importing power plant operation data: {e}")
            return 0
    
    def _save_to_db(self, df: pd.DataFrame) -> None:
        """
        Save data to the database.
        
        Args:
            df: DataFrame to save
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to save")
            return
        
        try:
            # Use DuckDBConnection in a context manager
            with DuckDBConnection(db_path=self.db_path) as conn:
                # Ensure the table exists
                self._ensure_table_exists(conn)
                
                # Insert data
                conn.execute(
                    """
                    INSERT INTO occto_plant_operation 
                    SELECT * FROM df
                    """,
                    {"df": df}
                )
                
                logger.info(f"Successfully inserted {len(df)} rows into occto_plant_operation")
        
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            raise
    
    def _ensure_table_exists(self, conn: Any) -> None:
        """
        Ensure the necessary table exists in the database.
        
        Args:
            conn: Database connection
        """
        try:
            # Check if table exists
            result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='occto_plant_operation'")
            
            if not result.fetchone():
                logger.info("Creating occto_plant_operation table")
                
                # Create the table
                conn.execute("""
                CREATE TABLE IF NOT EXISTS occto_plant_operation (
                    date DATE,
                    time VARCHAR,
                    area VARCHAR,
                    plant_name VARCHAR,
                    plant_type VARCHAR,
                    output_kw DECIMAL(18,6),
                    processing_date DATE,
                    PRIMARY KEY (date, time, plant_name)
                )
                """)
                
                logger.info("Table created successfully")
        
        except Exception as e:
            logger.error(f"Error ensuring table exists: {e}")
            raise 