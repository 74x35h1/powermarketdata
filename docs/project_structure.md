# Project Structure

## Overview
This project follows a modular structure with clear separation of concerns. The main components are:

```
powermarketdata/
├── config/                 # Configuration files
│   └── settings.yaml      # Project settings
├── data/                  # Data storage
│   └── raw/              # Raw data files
├── db/                    # Database related files
│   ├── duckdb_connection.py  # Database connection manager
│   └── schema_definition.sql # Database schema definition
├── data_sources/          # Data source modules
│   ├── jepx/             # JEPX data source
│   │   ├── __init__.py
│   │   └── jepx_bid.py   # JEPX bid data downloader
│   └── ...               # Other data sources
├── tests/                 # Test files
│   └── db_connection_test.py  # Database connection tests
├── .env                  # Environment variables
├── requirements.txt      # Python dependencies
└── README.md            # Project documentation
```

## Database Connection
The database connection is managed by the `DuckDBConnection` class in `db/duckdb_connection.py`. This class implements the Singleton pattern to ensure only one database connection exists throughout the application.

### Key Features:
- Singleton pattern implementation
- Automatic database creation if not exists
- Schema initialization
- Connection pooling
- Error handling

### Usage:
```python
from db.duckdb_connection import DuckDBConnection

# Get database connection
db = DuckDBConnection()
conn = db.get_connection()

# Execute query
result = db.execute_query("SELECT * FROM table_name")

# Close connection
db.close()
```

## Data Sources
Each data source is implemented as a separate module in the `data_sources` directory. Each module should:

1. Implement data downloading functionality
2. Handle data preprocessing
3. Save data to the database
4. Include proper error handling
5. Follow the project's coding standards

## Configuration
- Database path is configured in `.env` file using `DB_PATH` variable
- Project settings are stored in `config/settings.yaml`
- Environment variables are loaded using `python-dotenv`

## Testing
- Database connection tests are in `tests/db_connection_test.py`
- Each data source should have its own test module
- Tests should be run using pytest

## Dependencies
- Python 3.8+
- duckdb
- python-dotenv
- requests
- PyYAML
- pytest (for testing) 