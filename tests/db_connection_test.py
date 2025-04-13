#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from db.duckdb_connection import DuckDBConnection

def test_db_connection():
    """Test database connection and basic operations."""
    try:
        # Create a new connection
        db = DuckDBConnection()
        conn = db.get_connection()
        
        # Test connection by executing a simple query
        result = conn.execute("SELECT 1 as test").fetchall()
        
        # Verify the result
        assert result == [(1,)], "Database connection test failed"
        print("Database connection test passed successfully!")
        
        # Test schema initialization by checking if tables exist
        tables = conn.execute("SHOW TABLES").fetchall()
        print("Existing tables:", tables)
        
    except Exception as e:
        print(f"Database connection test failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection
        db.close()

if __name__ == "__main__":
    test_db_connection()