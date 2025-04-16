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
│   ├── schema_definition.sql # Database schema definition
│   └── __init__.py        # Database package initialization
├── data_sources/          # Data source modules
│   ├── __init__.py        # Data sources package initialization
│   ├── jepx/             # JEPX data source
│   │   ├── __init__.py    # JEPX package initialization
│   │   └── jepx_bid.py    # JEPX bid data downloader
│   └── ...               # Other data sources
├── exporter/              # Data export modules
├── ingestion/             # Data ingestion modules
├── transformation/        # Data transformation modules
├── cli/                   # Command-line interface modules
├── tests/                 # Test files
│   ├── db_connection_test.py  # Database connection tests
│   ├── test_jepx.py       # JEPX data source tests
│   └── test_jepx_db.py    # JEPX database integration tests
├── logs/                  # Log files
├── exports/               # Exported data files
├── .env                   # Environment variables
├── requirements.txt       # Python dependencies
├── main.py                # Main application entry point
└── README.md              # Project documentation
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

### JEPX Data Source
The JEPX (Japan Electric Power Exchange) data source provides functionality to download and process bid data from the JEPX website.

#### Usage:
```python
from data_sources.jepx import JEPXBidDownloader
from datetime import datetime

# Create downloader instance
downloader = JEPXBidDownloader()

# Download and save data for a date range
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 7)
downloader.download_and_save(start_date, end_date)
```

## Configuration
- Database path is configured in `.env` file using `DB_PATH` variable
- Environment variables are loaded using `python-dotenv`

## Testing
- Database connection tests are in `tests/db_connection_test.py`
- JEPX data source tests are in `tests/test_jepx.py` and `tests/test_jepx_db.py`
- Tests should be run using pytest or directly executing the test scripts

## Dependencies
- Python 3.8+
- duckdb
- python-dotenv
- requests
- PyYAML
- pytest (for testing) 