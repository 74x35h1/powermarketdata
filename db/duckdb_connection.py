# db/duckdb_connection.py
import os
import atexit
from pathlib import Path
from typing import Optional, Tuple, Union, Dict, Any

import duckdb
import pandas as pd
import numpy as np
import tempfile
import time
import logging

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class DuckDBConnection:
    """
    Context‑manager friendly wrapper around duckdb.connect.

    * 既定では書き込み可能な接続を取得します。
    * with 文で囲むと確実に close され、DuckDB のロックが残りません。
    * read‑only 接続を取りたい場合は `read_only=True` を渡してください。
    """

    def __init__(
        self,
        db_path: Optional[Union[str, Path]] = None,
        read_only: bool = False,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        load_dotenv()

        if db_path is None:
            self.db_path = os.getenv(
                "DB_PATH",
                "/default/path/should/not/be/used/if/dotenv/works/power_market_data.duckdb"
            )
            logger.info(f"DB path not provided directly. Resolved path: {self.db_path}")
            if self.db_path == "/default/path/should/not/be/used/if/dotenv/works/power_market_data.duckdb":
                logger.warning("Using hardcoded fallback DB_PATH. Ensure .env file is configured correctly or DB_PATH environment variable is set.")
        else:
            self.db_path = str(db_path)
            logger.info(f"DB path provided directly: {self.db_path}")

        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                logger.info(f"Ensured directory exists: {db_dir}")
            except OSError as e:
                logger.error(f"Could not create directory {db_dir}: {e}", exc_info=True)
        elif not db_dir:
             logger.info(f"Database path is in the current directory or is a special path (e.g., :memory:). Path: {self.db_path}")

        self.read_only = read_only
        self.config = config or {}
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

        atexit.register(self.close)

    # ------------------------------------------------------------------ #
    # Context‑manager support
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "DuckDBConnection":
        if self._connection is None:
            logger.info(f"Attempting to connect to DuckDB: {self.db_path} (read_only={self.read_only}, config={self.config})")
            try:
                self._connection = duckdb.connect(
                    database=self.db_path,
                    read_only=self.read_only,
                    config=self.config
                )
                logger.info(f"Successfully opened DuckDB connection: {self.db_path}")
            except Exception as e:
                logger.error(f"Failed to open DuckDB connection to {self.db_path}: {e}", exc_info=True)
                raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Public helpers
    # ------------------------------------------------------------------ #
    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        commit: bool = True,
    ) -> duckdb.DuckDBPyRelation:
        self._ensure_connection()

        try:
            logger.debug(
                f"Executing query on {'RO' if self.read_only else 'RW'} DB ({self.db_path}): {query[:120]}"
                f"{' with params' if params else ''}"
            )
            if params is not None:
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            if commit and not self.read_only:
                self._connection.commit()
                logger.debug(f"Committed transaction for query: {query[:50]}...")
            return result
        except Exception as e:
            logger.error(f"Error executing query: {query[:120]}... Error: {e}", exc_info=True)
            raise

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
                logger.info(f"Closed DuckDB connection: {self.db_path}")
            except Exception as e:
                logger.warning(f"Failed to close DuckDB connection ({self.db_path}): {e}", exc_info=True)
            finally:
                self._connection = None
    
    def is_connected(self) -> bool:
        return self._connection is not None

    def register(self, view_name: str, df: pd.DataFrame):
        self._ensure_connection()
        try:
            self._connection.register(view_name, df)
            logger.debug(f"Registered DataFrame as view: '{view_name}'")
        except Exception as e:
            logger.error(f"Failed to register view '{view_name}': {e}", exc_info=True)
            raise
            
    def drop_view(self, view_name: str):
        self._ensure_connection()
        try:
            self.execute_query(f'DROP VIEW IF EXISTS "{view_name}"', commit=True)
            logger.debug(f"Dropped view if exists: \"{view_name}\"")
        except Exception as e:
            logger.warning(f"Failed to drop view \"{view_name}\": {e}", exc_info=True)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _ensure_connection(self) -> None:
        if self._connection is None:
            logger.info(f"Lazily opening DuckDB connection: {self.db_path} (read_only={self.read_only}, config={self.config})")
            try:
                self._connection = duckdb.connect(
                    database=self.db_path,
                    read_only=self.read_only,
                    config=self.config
                )
                logger.info(f"Successfully opened DuckDB connection (lazy): {self.db_path}")
            except Exception as e:
                logger.error(f"Failed to lazily open DuckDB connection to {self.db_path}: {e}", exc_info=True)
                raise
            
    def save_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'append',
        check_duplicate_master_key: bool = True
    ) -> int:
        inserted_rows = 0
        if if_exists != 'append':
            logger.error(f"save_dataframe currently only supports if_exists='append', but got '{if_exists}'")
            raise ValueError("save_dataframe currently only supports if_exists='append'")

        if df.empty:
            logger.warning(f"DataFrame to save to table '{table_name}' is empty. No rows inserted.")
            return 0
            
        if check_duplicate_master_key and 'master_key' not in df.columns:
            logger.error(f"Duplicate check enabled but 'master_key' column missing in DataFrame for table '{table_name}'.")
            raise ValueError(f"DataFrame for table '{table_name}' is missing 'master_key' column required for duplicate check.")
        
        if check_duplicate_master_key and df['master_key'].isna().any():
            null_count = df['master_key'].isna().sum()
            logger.warning(f"'master_key' column in DataFrame for table '{table_name}' has {null_count} NULL values. These rows will be excluded.")
            df = df[df['master_key'].notna()].reset_index(drop=True)
            if df.empty:
                logger.warning(f"DataFrame for table '{table_name}' became empty after removing rows with NULL 'master_key'. No rows inserted.")
                return 0

        temp_view_name = f"temp_view_{table_name}_{int(time.time() * 1000)}_{os.getpid()}"
        
        quoted_table_name = f'"{table_name}"'
        quoted_temp_view_name = f'"{temp_view_name}"'
        
        conflict_target_column = "master_key"

        try:
            self._ensure_connection()
            
            try:
                result = self._connection.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
                ).fetchone()
                if not result:
                     logger.warning(f"Table '{table_name}' does not appear to exist in information_schema.tables. "
                                    "It's assumed to be created by schema definitions elsewhere, or this check might be incomplete.")
            except Exception as e:
                logger.warning(f"Could not robustly verify existence of table '{table_name}' using information_schema: {e}")

            self._connection.register(temp_view_name, df)
            logger.info(f"Registered DataFrame ({len(df)} rows) as temporary view '{temp_view_name}' for table {quoted_table_name}.")

            quoted_cols_list = [f'"{c}"' for c in df.columns]
            cols_sql_fragment = ", ".join(quoted_cols_list)

            if check_duplicate_master_key:
                insert_query = f"""
                INSERT INTO {quoted_table_name} ({cols_sql_fragment})
                SELECT {cols_sql_fragment} FROM {quoted_temp_view_name}
                ON CONFLICT ("{conflict_target_column}") DO NOTHING;
                """
                logger.info(f"Executing INSERT ON CONFLICT query for table {quoted_table_name} from view {quoted_temp_view_name}.")
            else:
                insert_query = f"INSERT INTO {quoted_table_name} ({cols_sql_fragment}) SELECT {cols_sql_fragment} FROM {quoted_temp_view_name};"
                logger.info(f"Executing INSERT query for table {quoted_table_name} from view {quoted_temp_view_name}.")
            
            try:
                self.execute_query(insert_query, commit=False)
                
                changes_result = self.execute_query("SELECT changes();", commit=False).fetchone()
                if changes_result:
                    inserted_rows = changes_result[0]
                
                self._connection.commit()
                logger.info(f"Successfully inserted {inserted_rows} rows into table {quoted_table_name}. (Attempted: {len(df)})")

            except duckdb.Error as e:
                logger.error(f"Database error during insert into {quoted_table_name} from {quoted_temp_view_name}: {e}", exc_info=True)
                try:
                    self._connection.rollback()
                    logger.info("Rolled back transaction due to error.")
                except Exception as rb_e:
                    logger.error(f"Failed to rollback transaction: {rb_e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error during insert into {quoted_table_name} from {quoted_temp_view_name}: {e}", exc_info=True)
                try:
                    self._connection.rollback()
                    logger.info("Rolled back transaction due to unexpected error.")
                except Exception as rb_e:
                    logger.error(f"Failed to rollback transaction: {rb_e}")
                raise
        
        finally:
            try:
                self.drop_view(temp_view_name)
            except Exception as e_drop:
                logger.warning(f"Could not drop temporary view {temp_view_name}: {e_drop}")

        return inserted_rows

# Example usage of the context manager
if __name__ == "__main__":
    # Example 1: Using with statement for write operations
    with DuckDBConnection() as db:
        db.execute_query("INSERT INTO my_table (column1, column2) VALUES (?, ?)", (123, "test"))
        print("Data inserted successfully")
    # Connection is automatically closed when the with block exits
    
    # Example 2: Using with statement for read-only operations
    with DuckDBConnection(read_only=True) as db:
        result = db.execute_query("SELECT * FROM my_table LIMIT 10")
        for row in result.fetchall():
            print(row)
    # Connection is automatically closed when the with block exits