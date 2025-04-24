# db/duckdb_connection.py
import os
import atexit
from pathlib import Path
from typing import Optional, Tuple, Union

import duckdb


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
        *,
        read_only: bool = False,
    ) -> None:
        self.db_path = (
            str(db_path)
            if db_path is not None
            else os.getenv(
                "DB_PATH",
                "/Volumes/MacMiniSSD/powermarketdata/power_market_data",
            )
        )
        # フォルダが無い場合は作成
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)

        self.read_only = read_only
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

        # atexit で確実にクローズ
        atexit.register(self.close)

    # ------------------------------------------------------------------ #
    # Context‑manager support
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "DuckDBConnection":
        if self._connection is None:
            self._connection = duckdb.connect(
                self.db_path,
                read_only=self.read_only,
            )
            print(f"[INFO] Opened DuckDB connection: {self.db_path} (read_only={self.read_only})")
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
        """
        Execute a SQL query and return its relation.

        Parameters
        ----------
        query : str
            SQL statement.
        params : tuple, optional
            Parameters for parametrised query.
        commit : bool
            Whether to call commit() after executing. Ignored on read‑only
            connections.
        """
        self._ensure_connection()

        try:
            print(
                f"[DEBUG] DuckDB ({'RO' if self.read_only else 'RW'}) {self.db_path} : "
                f"{query[:120]}"
            )
            if params is not None:
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            if commit and not self.read_only:
                self._connection.commit()

            return result
        except Exception as e:
            print(f"[ERROR] Error executing query: {e}")
            raise  # re‑raise so caller can see the stack

    def close(self) -> None:
        """Close the underlying DuckDB connection (if it exists)."""
        if self._connection is not None:
            try:
                self._connection.close()
                print(f"[INFO] Closed DuckDB connection: {self.db_path}")
            except Exception as e:
                print(f"[WARN] Failed to close DuckDB connection: {e}")
            finally:
                self._connection = None
    
    def is_connected(self) -> bool:
        """Check if database connection is currently open."""
        return self._connection is not None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _ensure_connection(self) -> None:
        """Open a connection lazily if it doesn't exist yet."""
        if self._connection is None:
            self._connection = duckdb.connect(
                self.db_path,
                read_only=self.read_only,
            )
            print(f"[INFO] Opened DuckDB connection: {self.db_path} (read_only={self.read_only})")
            
    # Keep backward compatibility with existing code
    def save_dataframe(self, df, table_name, if_exists='append'):
        """Save a dataframe to a table"""
        self._ensure_connection()
        
        if if_exists == 'replace':
            self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
            
        # Create table if it doesn't exist, append otherwise
        if df is not None and len(df) > 0:
            # Convert to proper format for DuckDB
            table_query = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0"
            self.execute_query(table_query)
            
            # Insert data
            insert_query = f"INSERT INTO {table_name} SELECT * FROM df"
            self._connection.register("df", df)
            self.execute_query(insert_query)
            return len(df)
        return 0

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