#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JMA Weather Data DB Importer

Imports JMA weather data from processed CSV files into a DuckDB database.
"""

import os
import sys
import logging
import pandas as pd
import glob
import re
from pathlib import Path
import argparse

# Add project root to Python path for module imports
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import DuckDBConnection
try:
    from db.duckdb_connection import DuckDBConnection
except ImportError as e:
    print(f"Error importing DuckDBConnection: {e}", file=sys.stderr)
    print("Please ensure db/duckdb_connection.py exists and the project structure is correct.", file=sys.stderr)
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout) # Ensure logs go to stdout
    ]
)
logger = logging.getLogger(__name__)

class JMAWeatherDBImporter:
    """Imports JMA weather data from CSV files into the DuckDB database."""

    def __init__(self, db_path: str = None, read_only: bool = False):
        """Initializes the importer and ensures the target table exists."""
        logger.info(f"Initializing JMAWeatherDBImporter with db_path: {db_path}")
        try:
            self.connection = DuckDBConnection(db_path, read_only=read_only)
            self.table_name = "jma_weather"
            self._ensure_table()
        except Exception as e:
            logger.error(f"Error during JMAWeatherDBImporter initialization: {e}", exc_info=True)
            raise # Re-raise the exception to indicate failure

    def _ensure_table(self):
        """Ensures the jma_weather table exists by executing the relevant part of the schema file."""
        try:
            # Get project root robustly
            project_root = Path(__file__).resolve().parent.parent.parent
            schema_path = project_root / "db" / "schema_definition.sql"
            logger.info(f"Reading schema definition: {schema_path}")

            if not schema_path.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(schema_path, "r", encoding='utf-8') as f:
                full_schema_sql = f.read()

            # Extract the specific CREATE TABLE statement for jma_weather
            # Regex tries to capture the entire statement until the semicolon
            match = re.search(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+" + self.table_name + r"\s*\((.*?)\);",
                full_schema_sql,
                re.IGNORECASE | re.DOTALL | re.MULTILINE
            )

            if not match:
                raise ValueError(f"Could not find CREATE TABLE statement for {self.table_name} in {schema_path}")

            create_table_sql = match.group(0) # Get the full matched statement
            logger.info(f"Ensuring table {self.table_name} exists...")
            # logger.debug(f"Executing schema: \n{create_table_sql}")
            self.connection.execute_query(create_table_sql)
            logger.info(f"Table {self.table_name} ensured in the database.")

        except Exception as e:
            logger.error(f"Failed to ensure table {self.table_name}: {e}", exc_info=True)
            raise

    def load_csvs_to_dataframe(self, csv_dir: str) -> pd.DataFrame | None:
        """Loads and concatenates JMA weather CSVs from a directory."""
        csv_pattern = os.path.join(csv_dir, "jma_weather_*.csv")
        csv_files = sorted(glob.glob(csv_pattern)) # Sort for consistent processing order

        if not csv_files:
            logger.warning(f"No CSV files found matching pattern: {csv_pattern}")
            return None

        logger.info(f"Found {len(csv_files)} CSV files to load. First file: {csv_files[0]}")
        all_dfs = []
        
        # Define expected columns based on the schema
        schema_cols = [
            'primary_key', 'station_id', 'date', 'time', 'temperature',
            'sunshine_duration', 'global_solar_radiation', 'wind_speed',
            'wind_direction_sin', 'wind_direction_cos',
            'weather_description', 'snowfall_depth'
        ]
        
        # Define dtypes for reading CSV to prevent issues
        read_dtypes = {
            'primary_key': str,
            'station_id': str,
            'date': str,
            'time': str,
            'temperature': float, # Read as float, DuckDB handles DECIMAL
            'sunshine_duration': float,
            'global_solar_radiation': float,
            'wind_speed': float,
            'wind_direction_sin': float,
            'wind_direction_cos': float,
            'weather_description': str,
            'snowfall_depth': float
        }

        for f in csv_files:
            try:
                logger.debug(f"Loading CSV: {f}")
                # Read CSV using specified dtypes
                df = pd.read_csv(
                    f,
                    dtype=read_dtypes,
                    keep_default_na=True,
                    na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null', '--'] # Added common missing value strings
                )
                logger.debug(f"Loaded {len(df)} rows from {f}")

                # Check if all required columns exist after load
                missing_in_csv = [col for col in schema_cols if col not in df.columns]
                if missing_in_csv:
                    logger.warning(f"Columns missing in CSV {f}: {missing_in_csv}. They will be added with NA.")
                    for col in missing_in_csv:
                        df[col] = pd.NA # Add missing columns
                
                # Select only the columns defined in the schema to ensure consistency
                df = df[schema_cols]

                # Final type check/conversion (redundant if read_dtypes works, but safe)
                for col, dtype in read_dtypes.items():
                    if col in df.columns:
                        if pd.api.types.is_numeric_dtype(dtype) and not pd.api.types.is_numeric_dtype(df[col].dtype):
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_string_dtype(df[col].dtype):
                             # Handle potential NaN values correctly when converting to string
                             df[col] = df[col].astype(str).replace('nan', '').replace('<NA>', '')
                
                # Handle potential string 'nan' or '<NA>' in description after conversion
                if 'weather_description' in df.columns:
                    df['weather_description'] = df['weather_description'].astype(str).replace('nan', '').replace('<NA>', '')


                all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error loading or processing CSV file {f}: {e}", exc_info=True)
                continue # Skip problematic files

        if not all_dfs:
            logger.error("No data loaded from CSV files after processing.")
            return None

        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Combined DataFrame created with {len(combined_df)} rows.")
        
        # Drop rows with NA primary key, as they cannot be inserted
        initial_rows = len(combined_df)
        combined_df.dropna(subset=['primary_key'], inplace=True)
        if len(combined_df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(combined_df)} rows with missing primary_key.")

        # Final check for duplicates based on primary key before returning
        duplicates = combined_df[combined_df.duplicated(subset=['primary_key'], keep=False)]
        if not duplicates.empty:
            logger.warning(f"Found {len(duplicates)} duplicate primary_key rows in the combined CSV data. Keeping the first occurrence.")
            combined_df = combined_df.drop_duplicates(subset=['primary_key'], keep='first')
            logger.info(f"DataFrame size after dropping duplicates: {len(combined_df)} rows.")
        
        if combined_df.empty:
            logger.warning("DataFrame is empty after processing and deduplication.")
            return None

        return combined_df

    def import_dataframe(self, df: pd.DataFrame) -> int:
        """Imports the DataFrame into the jma_weather table using INSERT OR IGNORE."""
        if df is None or df.empty:
            logger.warning("DataFrame is empty. Nothing to import.")
            return 0

        temp_view_name = f"temp_{self.table_name}_import_{os.getpid()}" # Add PID for potential concurrency
        rows_inserted = 0
        
        # Ensure column order matches schema for INSERT statement (important!)
        schema_cols = [
            'primary_key', 'station_id', 'date', 'time', 'temperature',
            'sunshine_duration', 'global_solar_radiation', 'wind_speed',
            'wind_direction_sin', 'wind_direction_cos',
            'weather_description', 'snowfall_depth'
        ]
        df = df[schema_cols] # Reorder DataFrame columns to match
        
        try:
            logger.info(f"Registering DataFrame with {len(df)} rows as temporary view '{temp_view_name}'...")
            self.connection.register(temp_view_name, df)
            logger.info("DataFrame registered.")

            # Use INSERT ... ON CONFLICT DO NOTHING
            cols = ", ".join([f'"{c}"' for c in df.columns]) # Quote column names
            sql = f"""
            INSERT INTO "{self.table_name}" ({cols})
            SELECT {cols} FROM "{temp_view_name}"
            ON CONFLICT (primary_key) DO NOTHING;
            """
            logger.info(f"Executing INSERT ON CONFLICT DO NOTHING query for table {self.table_name}...")
            self.connection.execute_query(sql) # Assume execute_query handles commit
            
            # Try to get the number of changed rows using DuckDB's changes() function
            try:
                changes_result = self.connection.execute_query("SELECT changes();")
                rows_inserted = changes_result.fetchone()[0]
                logger.info(f"DuckDB reports {rows_inserted} rows were actually inserted into {self.table_name}.")
            except Exception as change_err:
                logger.warning(f"Could not get exact inserted row count using changes(): {change_err}. Import likely succeeded.")
                # Can't be sure how many were inserted vs ignored
                rows_inserted = -1 # Indicate uncertainty

            # Unregister the temporary view
            logger.debug(f"Unregistering temporary view '{temp_view_name}'...")
            self.connection.drop_view(temp_view_name)
            logger.debug(f"Temporary view '{temp_view_name}' unregistered.")

        except Exception as e:
            logger.error(f"Error importing DataFrame to {self.table_name}: {e}", exc_info=True)
            # Attempt to unregister view even if import fails
            try:
                self.connection.drop_view(temp_view_name)
            except Exception as e_unreg:
                logger.error(f"Failed to unregister temporary view '{temp_view_name}' after error: {e_unreg}")
            return 0 # Return 0 on failure

        if rows_inserted >= 0:
            logger.info(f"Import process finished for {self.table_name}. Rows inserted: {rows_inserted}.")
        else:
             logger.info(f"Import process finished for {self.table_name}. Query executed, but exact inserted count unavailable.")
        # Return inserted count if known, otherwise maybe total rows attempted or 0/1 for success?
        # Returning rows_inserted (-1 if unknown) seems informative.
        return rows_inserted

    # --- Context Management --- 
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Closes the database connection."""
        if self.connection:
            logger.info("Closing database connection...")
            self.connection.close()
            logger.info("Database connection closed.")

# --- Main execution block for standalone testing --- 
def main():
    parser = argparse.ArgumentParser(description="Import JMA Weather CSV data into DuckDB.")
    parser.add_argument(
        "--csv-dir", 
        type=str, 
        default="./jma_data_csv", 
        help="Directory containing the JMA weather CSV files (default: ./jma_data_csv)."
    )
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="power_market_data.db", 
        help="Path to the DuckDB database file (default: power_market_data.db)."
    )
    parser.add_argument(
        "--log-level",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO', 
        help="Set the logging level (default: INFO)"
    )
    
    args = parser.parse_args()

    # Update logger level
    logger.setLevel(args.log_level)
    # Also update handler level if necessary (though root logger level usually sufficient)
    for handler in logger.handlers:
        handler.setLevel(args.log_level)
        
    logger.info(f"Starting JMA Weather DB Import...")
    logger.info(f"CSV Directory: {args.csv_dir}")
    logger.info(f"Database Path: {args.db_path}")
    logger.info(f"Log Level: {args.log_level}")

    try:
        # Use context manager for the importer
        with JMAWeatherDBImporter(db_path=args.db_path) as importer:
            # Load data from CSVs
            weather_df = importer.load_csvs_to_dataframe(args.csv_dir)
            
            # Import the loaded data
            if weather_df is not None and not weather_df.empty:
                logger.info(f"Attempting to import {len(weather_df)} rows...")
                inserted_count = importer.import_dataframe(weather_df)
                if inserted_count >= 0:
                    logger.info(f"Import completed. Rows inserted: {inserted_count}")
                else:
                    logger.info("Import query executed, exact inserted count unknown.")
            elif weather_df is None:
                logger.error("Failed to load data from CSVs.")
            else: # DataFrame is empty
                 logger.warning("Loaded DataFrame is empty, nothing to import.")

    except FileNotFoundError as e:
         logger.critical(f"A required file was not found: {e}")
    except ValueError as e:
         logger.critical(f"A value error occurred, possibly during schema parsing or data conversion: {e}")
    except ImportError as e:
         # This was already caught earlier, but double-check
         logger.critical(f"Import error for required modules: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during the import process: {e}", exc_info=True)

    logger.info("JMA Weather DB Import finished.")

if __name__ == "__main__":
    main() 