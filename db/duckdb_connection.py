# db/duckdb_connection.py
import os
import duckdb
from dotenv import load_dotenv
from typing import Optional
from pathlib import Path

class DuckDBConnection:
    """DuckDB database connection manager (uses DB_PATH from .env or default)"""
    def __init__(self):
        self.db_path = os.getenv("DB_PATH", "/Volumes/MacMiniSSD/powermarketdata/power_market_data")
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self._connection = duckdb.connect(self.db_path)
        print(f"Connected to DuckDB at: {self.db_path}")

    def execute_query(self, query: str, params: tuple = None) -> Optional[duckdb.DuckDBPyRelation]:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            Optional[duckdb.DuckDBPyRelation]: Query result if successful, None otherwise
        """
        try:
            print(f"[DEBUG] DuckDBファイル: {os.path.abspath(self.db_path)} でクエリ実行: {query[:80]}")
            if params is not None:
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)
            self._connection.commit()  # 明示的にcommit
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self._connection:
            try:
                self._connection.close()
                print(f"[INFO] DuckDB接続を正常に閉じました: {self.db_path}")
            except Exception as e:
                print(f"[ERROR] DuckDB接続を閉じる際にエラーが発生: {str(e)}")
            finally:
                self._connection = None
        else:
            print(f"[INFO] DuckDB接続は既に閉じられています: {self.db_path}")