# db/duckdb_connection.py
import os
import duckdb
from dotenv import load_dotenv
from typing import Optional
from pathlib import Path

class DuckDBConnection:
    """Singleton class for managing DuckDB database connections."""
    
    _instance = None
    _connection = None
    _db_path = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckDBConnection, cls).__new__(cls)
            # Load environment variables
            load_dotenv()
            # Set database path
            cls._db_path = os.getenv("DB_PATH", "default_duckdb.db")
        return cls._instance

    def __init__(self):
        if self._connection is None:
            self._initialize_connection()

    def _initialize_connection(self):
        """Initialize the database connection and schema."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
            
            # Connect to DuckDB (creates the database if it doesn't exist)
            self._connection = duckdb.connect(self._db_path)
            print(f"Connected to DuckDB at: {self._db_path}")
            
            # Initialize schema
            self._initialize_schema()
        except Exception as e:
            print(f"Failed to initialize database connection: {e}")
            raise

    def _initialize_schema(self):
        """Initialize the database schema."""
        try:
            schema_path = os.path.join(os.path.dirname(__file__), "schema_definition.sql")
            with open(schema_path, "r") as f:
                schema_sql = f.read()
            self._connection.execute(schema_sql)
        except Exception as e:
            print(f"Failed to initialize schema: {e}")
            raise

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get the database connection."""
        if self._connection is None:
            self._initialize_connection()
        return self._connection

    def execute_query(self, query: str) -> Optional[duckdb.DuckDBPyRelation]:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            Optional[duckdb.DuckDBPyRelation]: Query result if successful, None otherwise
        """
        try:
            return self.get_connection().execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None