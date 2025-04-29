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
import time
import duckdb

# Removed DuckDB specific imports
# from db.duckdb_connection import DuckDBConnection
# from data_sources.occto.occto_downloader import OCCTODownloader

logger = logging.getLogger(__name__)

# Define the database file path (user specified)
DEFAULT_DB_PATH = "/Volumes/MacMiniSSD/powermarketdata/power_market_data"
# Define the table name
TABLE_NAME = "occto_30min_generation"

class OCCTO30MinDBImporter:
    """
    Class for importing OCCTO 30-minute generation data into the DuckDB database.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the OCCTO 30-minute data importer for DuckDB.
        
        Args:
            db_path: Path to the DuckDB database file. If None, uses DEFAULT_DB_PATH.
        """
        # Use DuckDBConnection
        self.db_path = db_path if db_path else DEFAULT_DB_PATH
        # Instantiate DuckDBConnection but don't connect yet (handled by context manager or ensure_connection)
        self.connection = DuckDBConnection(db_path=self.db_path, read_only=False) 
        logger.info(f"Using DuckDB database: {self.db_path}")
        # Ensure schema on initialization within a try-except block
        try:
            logger.debug("Attempting to initialize schema...")
            with self.connection: # Use context manager to ensure connection
                 self._initialize_schema()
            logger.debug("Schema initialization process completed or skipped existing tables.")
        except Exception as e:
             logger.error(f"Failed during schema initialization in __init__: {e}")
             # Re-raise the exception to stop the process if schema fails
             raise e
    
    def _initialize_schema(self):
        """Read schema_definition.sql and execute it using DuckDBConnection."""
        # This method is now called within the __init__ context manager
        try:
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema_definition.sql"
            if not schema_path.exists():
                logger.error(f"Schema definition file not found at: {schema_path}")
                # Raise error if schema file is missing
                raise FileNotFoundError(f"Schema definition file not found at: {schema_path}")

            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            if not schema_sql.strip():
                 logger.error("Schema definition file is empty.")
                 raise ValueError("Schema definition file is empty.")

            # Execute the entire script at once
            logger.info("Executing entire schema definition script...")
            # Connection is already established by the __init__ context manager
            self.connection.execute_query(schema_sql) # Execute the whole script
            # DuckDB often handles CREATE TABLE IF NOT EXISTS atomically or transactionally per statement
            # Explicit commit might not be necessary after successful execution,
            # as execute_query defaults to commit=True.
            logger.info("Schema definition script executed successfully.")

        except FileNotFoundError as e:
             logger.error(f"Schema file error: {e}")
             raise # Re-raise FileNotFoundError
        except Exception as e:
            # Catch potential DuckDB errors during execution of the whole script
            logger.error(f"Error executing schema definition script: {e}")
            # Re-raise the exception to ensure __init__ fails
            raise

    def transform_occto_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame from the OCCTO downloader to the DB schema.
        Includes master_key generation, date formatting, and slot mapping.
        
        Args:
            df: Raw DataFrame from OCCTO JSON response.
        
        Returns:
            Transformed DataFrame ready for DB insertion.
        """
        if df.empty:
            return pd.DataFrame() # Return empty if input is empty

        logger.info(f"Starting transformation of {len(df)} rows...")
        
        # Make a copy to avoid modifying the original DataFrame
        transformed_df = df.copy()

        # 1. Basic Rename columns (excluding time slots for now)
        column_mapping = {
            'htdnsCd': 'plant_code', 
            'areaCd': 'area_code',
            'unitNm': 'unit_num',
            'tgtDate': 'date', # Keep 'date' for now, format later
            'htdnsNm': 'plant_name',
            'htdnNm': 'gen_method',
            'dayElr': 'total' # Assuming dayElr is the correct total column
        }
        transformed_df.rename(columns=column_mapping, inplace=True)
        
        # 2. Rename and map time slot columns (htdnRsltXXXX -> slotY)
        # Correct mapping: htdnRslt0030->slot1, htdnRslt0100->slot2, ..., htdnRslt2400->slot48
        time_slot_mapping = {}
        # Correctly generate the 48 time suffixes corresponding to slots 1 to 48 (Revised Logic)
        time_suffixes = []
        for slot_num in range(1, 49): # Loop from 1 to 48
            if slot_num == 48:        # Slot 48 is always 24:00
                time_str = "2400"
            elif slot_num % 2 == 1: # Odd slots (1, 3, ..., 47) are XX:30
                hour = (slot_num - 1) // 2
                time_str = f"{hour:02d}30"
            else:                   # Even slots (2, 4, ..., 46) are XX:00
                hour = slot_num // 2
                time_str = f"{hour:02d}00"
            time_suffixes.append(time_str)
        # Expected: ['0030', '0100', '0130', '0200', ..., '2330', '2400']
        
        for i, suffix in enumerate(time_suffixes):
            slot_index = i + 1
            original_col = f'htdnRslt{suffix}'
            new_col = f'slot{slot_index}'
            if original_col in transformed_df.columns:
                time_slot_mapping[original_col] = new_col
            else:
                 # Log warning but proceed, missing columns will result in NaN/Null later
                 logger.warning(f"Original time column '{original_col}' (for {new_col}) not found in DataFrame.")
        
        # Add debug logging before renaming
        logger.debug(f"Original columns before time slot rename: {transformed_df.columns.tolist()}")
        logger.debug(f"Time slot mapping dictionary: {time_slot_mapping}")
        
        transformed_df.rename(columns=time_slot_mapping, inplace=True)

        # 3. Data type and value transformations
        try:
            # Convert date string 'YYYY/MM/DD' to 'YYYYMMDD' TEXT
            # Store the YYYYMMDD date first before using it for master_key
            transformed_df['date'] = pd.to_datetime(transformed_df['date'], format='%Y/%m/%d').dt.strftime('%Y%m%d')
            
            # Create master_key: YYYYMMDD_plantcode_unitnum 
            # Ensure components are strings and handle potential None/NaN
            transformed_df['master_key'] = (
                transformed_df['date'].astype(str) + '_' + 
                transformed_df['plant_code'].fillna('NA').astype(str) + '_' + 
                transformed_df['unit_num'].fillna('NA').astype(str)
            )

            # Convert area_code '01' to integer 1
            if 'area_code' in transformed_df.columns:
                transformed_df['area_code'] = pd.to_numeric(transformed_df['area_code'], errors='coerce').astype('Int64') # Use Int64 for nullable integer

            # Map generation method names
            gen_method_mapping = {
                '原子力': 'nuclear',
                '火力（ガス）': 'LNG',
                '火力（石炭）': 'coal',
                '火力（石油）': 'oil',
                '水力': 'hydro',
                '地熱': 'geothermal',
                'その他': 'biomass', # Assuming その他 maps to biomass based on context
                '太陽光': 'solar',
                '風力': 'wind',
                '揚水': 'pumped_storage',
                '蓄電池': 'battery'
            }
            # Fill NaN/None before mapping to avoid errors, map, and handle unmapped (map to 'other_fire')
            if 'gen_method' in transformed_df.columns:
                transformed_df['gen_method'] = transformed_df['gen_method'].fillna('other_fire').astype(str)
                transformed_df['gen_method'] = transformed_df['gen_method'].map(gen_method_mapping).fillna('other_fire')
            
             # Convert time slot columns (slot1-slot48) and total to numeric (integer), coercing errors to 0
            slot_cols_to_convert = [f'slot{i+1}' for i in range(48)] + ['total']
            for col in slot_cols_to_convert:
                 if col in transformed_df.columns:
                     transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce').fillna(0).astype(int) # Coerce errors to 0
                 else:
                     # If a slot column is missing after renaming, create it with 0s
                     logger.warning(f"Column '{col}' not found after transformation. Creating with 0s.")
                     transformed_df[col] = 0 

        except Exception as e:
             logger.error(f"Error during data type conversion or master_key generation: {e}")
             import traceback
             traceback.print_exc()
             return pd.DataFrame()

        # 4. Select and reorder columns according to the new schema
        final_columns = ['master_key', 'date', 'plant_code', 'unit_num', 'area_code', 'plant_name', 'gen_method'] + \
                        [f'slot{i+1}' for i in range(48)] + ['total']
        # Ensure all required columns exist, fill missing ones if necessary (though should be handled above)
        for col in final_columns:
            if col not in transformed_df.columns:
                 logger.warning(f"Final column '{col}' is missing. This shouldn't happen.")
                 # Decide how to handle: add column with defaults or error out?
                 # Adding with None/0 for safety for now, but indicates upstream issue.
                 if col == 'master_key': continue # Should always exist
                 transformed_df[col] = 0 if col.startswith('slot') or col == 'total' else None 
                 
        transformed_df = transformed_df[final_columns] # Select and reorder

        logger.info("Transformation complete.")
        return transformed_df

    def get_latest_date(self) -> Optional[date]:
        """Retrieves the latest date present in the database table using DuckDB."""
        try:
            with self.connection: # Use context manager
                # Use DuckDBConnection's execute_query
                result = self.connection.execute_query(f"SELECT MAX(date) FROM {TABLE_NAME}")
                latest_date_str = result.fetchone()
                
                if latest_date_str and latest_date_str[0]:
                    # DuckDB might return date objects directly or strings
                    if isinstance(latest_date_str[0], date):
                         latest_date = latest_date_str[0]
                    else:
                         latest_date = datetime.strptime(latest_date_str[0], '%Y-%m-%d').date()
                    logger.info(f"Latest date in DB: {latest_date}")
                    return latest_date
                else:
                    logger.info("No existing data found in the table.")
                    return None
        except duckdb.CatalogException:
             # Handle case where table doesn't exist yet gracefully
             logger.warning(f"Table '{TABLE_NAME}' not found while checking latest date. Assuming empty.")
             return None
        except Exception as e:
             logger.error(f"Unexpected error getting latest date: {e}")
             return None # Or raise error

    def insert_occto_data(self, df: pd.DataFrame):
        """
        Inserts the transformed DataFrame into the DuckDB database, avoiding duplicates.
        Only inserts data newer than the latest date in the DB.
        Uses DuckDB's ON CONFLICT DO NOTHING.
        
        Args:
            df: Transformed DataFrame ready for DB insertion.
        """
        if df.empty:
            logger.info("No data to insert.")
            return

        latest_db_date = self.get_latest_date()

        # Filter DataFrame for dates newer than the latest in DB
        if latest_db_date:
            df_to_insert = df[df['date'] > latest_db_date.strftime('%Y-%m-%d')].copy()
        else:
            df_to_insert = df.copy()

        if df_to_insert.empty:
            logger.info("No new data found to insert based on date.")
            return

        logger.info(f"Attempting to insert {len(df_to_insert)} new rows into {TABLE_NAME}...")
        
        # Use a temporary view for efficient insertion
        temp_view_name = f"temp_occto_insert_{int(time.time() * 1000)}"
        
        try:
            with self.connection: # Ensure connection using context manager
                # Register DataFrame as a temporary view
                self.connection.register(temp_view_name, df_to_insert)
                
                # Construct the INSERT query with ON CONFLICT DO NOTHING for the new master_key
                cols = ', '.join([f'"{c}"' for c in df_to_insert.columns]) # Quote column names
                sql = f"""
                INSERT INTO {TABLE_NAME} ({cols}) 
                SELECT * FROM {temp_view_name}
                ON CONFLICT (master_key) DO NOTHING
                """
                
                logger.debug(f"Executing insert query: {sql[:200]}...")
                result = self.connection.execute_query(sql, commit=True) # Commit after insert
                
                # Get the number of rows affected (might require specific DuckDB relation methods)
                # For simplicity, just log the attempt size for now.
                # inserted_rows = result.rowcount # This might not work directly
                
                logger.info(f"DB insert operation finished for {TABLE_NAME}. Attempted to insert {len(df_to_insert)} rows.")

        except Exception as e:
             logger.error(f"Error during data insertion into {TABLE_NAME}: {e}")
             # Print traceback for detailed debugging
             import traceback
             traceback.print_exc()
        finally:
             # Clean up the temporary view if possible (might need specific method)
             try:
                 with self.connection:
                     self.connection.execute_query(f"DROP VIEW IF EXISTS {temp_view_name}")
             except Exception as e_drop:
                 logger.warning(f"Could not drop temporary view {temp_view_name}: {e_drop}")

# Example usage (can be run standalone for testing)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("Testing OCCTO DB Importer...")
    
    # Create a dummy DataFrame matching the raw OCCTO format for testing
    dummy_data = {
        'htdnsCd': ['10001', '13017', '10001'], 
        'areaCd': ['01', '01', '01'], 
        'unitNm': ['１号機', '１号機', '１号機'], 
        'tgtDate': ['2024/05/01', '2024/05/01', '2024/05/02'], 
        'areaCdNm': ['北海道', '北海道', '北海道'], 
        'htdnsNm': ['泊発電所', '京極発電所', '泊発電所'], 
        'htdnHsk': [None, None, None], 
        'htdnNm': ['原子力', '水力', '原子力'], 
        'htdnRslt0030': ['0', '12870', '0'], 
        'htdnRslt0100': ['0', '11880', '0'],
        # ... add other time columns up to htdnRslt2400 with dummy values ...
        'htdnRslt2400': ['0', '4950', '0'],
        'dayElr': ['0', '402930', '0'], 
        'upyh': ['2024/05/02 15:31', '2024/05/02 15:31', '2024/05/03 15:31'], 
        'upyhSaveCSV': ['2024/05/02 15:31:01', '2024/05/02 15:31:01', '2024/05/03 15:31:01']
    }
    # Add dummy time columns t3-t47
    for i in range(2, 48):
        hour = i // 2
        minute = (i % 2) * 30
        time_str = f"{hour:02d}{minute:02d}"
        original_col = f'htdnRslt{time_str}'
        dummy_data[original_col] = ['0'] * 3
        
    raw_df = pd.DataFrame(dummy_data)

    importer = OCCTO30MinDBImporter()
    transformed_df = importer.transform_occto_data(raw_df)
    
    print("\nTransformed DataFrame sample:")
    print(transformed_df.head())
    
    print("\nInserting data into database...")
    importer.insert_occto_data(transformed_df)
    
    print("\nChecking latest date after insert...")
    latest = importer.get_latest_date()
    print(f"Latest date: {latest}")
    
    print("\nAttempting to insert the same data again (should be ignored)...")
    importer.insert_occto_data(transformed_df)
    
    # Test inserting slightly newer data
    dummy_data_new = dummy_data.copy()
    dummy_data_new['tgtDate'] = ['2024/05/03'] * 3
    raw_df_new = pd.DataFrame(dummy_data_new)
    transformed_df_new = importer.transform_occto_data(raw_df_new)
    print("\nInserting newer data...")
    importer.insert_occto_data(transformed_df_new)
    latest = importer.get_latest_date()
    print(f"Latest date after new insert: {latest}")

    logger.info("DB Importer test finished.") 