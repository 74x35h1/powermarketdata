"""
Database package for Power Market Data project.
This package contains database connection and schema management components.
"""

from .duckdb_connection import DuckDBConnection

__all__ = ['DuckDBConnection']
