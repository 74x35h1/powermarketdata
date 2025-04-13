# Power Market Data Project Structure

## Overview
This project is designed to collect, process, and manage various power market data from multiple sources including JEPX, OCCTO, TSO, JMA, and various futures markets. The system is built with a modular architecture to ensure scalability and maintainability.

## Directory Structure
```
powermarketdata/
├─ main.py                     # CLI entry point
├─ cli/
│   └─ menu.py                 # CLI menu and selection logic
├─ config/
│   └─ settings.yaml           # Configuration management
├─ data_sources/               # Data collection modules
│   ├─ jepx_price.py           # JEPX spot price data
│   ├─ jepx_bid.py             # JEPX bid data
│   ├─ hjks.py                 # HJKS data
│   ├─ jma_weather.py          # JMA weather data
│   ├─ tso_demand.py           # TSO demand data
│   ├─ occto_interconnection.py# OCCTO interconnection data
│   ├─ occto_reserve.py        # OCCTO reserve data
│   ├─ eex_futures.py          # EEX futures data
│   ├─ tocom_futures.py        # TOCOM futures data
│   ├─ ice_jkm.py              # ICE JKM futures
│   ├─ ice_ttf.py              # ICE TTF futures
│   ├─ ice_nc_coal.py          # ICE NC coal futures
│   ├─ ice_dubai_crude.py      # ICE Dubai crude futures
│   └─ ice_usdjpy_futures.py   # ICE USD/JPY futures
├─ db/
│   ├─ duckdb_connection.py    # DuckDB connection management
│   └─ schema_definition.sql   # Database schema
├─ ingestion/                  # ETL pipeline
│   ├─ importer.py             # File import functionality
│   └─ loader.py               # Data loading to DB
├─ transformation/             # Data transformation
│   └─ ml_dataset_builder.py   # ML dataset creation
├─ exporter/                   # Data export
│   └─ export_dataset.py       # Dataset export functionality
└─ tests/                      # Test modules
    └─ test_jepx.py            # JEPX module tests
```

## Module Dependencies and Relationships

### 1. CLI Layer
```
main.py
└─ cli/menu.py
   ├─ config/settings.yaml
   └─ data_sources/*
```
- **Dependencies**: 
  - All data source modules
  - PyYAML (configuration)
- **Responsibilities**:
  - User interaction
  - Data source selection
  - Command execution

### 2. Data Source Modules
```
data_sources/
├─ jepx_price.py
│  ├─ requests
│  ├─ pandas
│  └─ db/duckdb_connection.py
├─ jepx_bid.py
│  ├─ requests
│  ├─ pandas
│  └─ db/duckdb_connection.py
└─ futures/
   ├─ eex_futures.py
   │  ├─ requests
   │  ├─ pandas
   │  └─ db/duckdb_connection.py
   └─ ice_*.py
      ├─ requests
      ├─ pandas
      └─ db/duckdb_connection.py
```
- **Dependencies**:
  - Requests (HTTP client)
  - Pandas (data manipulation)
  - DuckDB (database)
- **Output**: Raw market data
- **Used by**: Ingestion layer

### 3. Database Layer
```
db/
├─ duckdb_connection.py
│  ├─ duckdb
│  └─ config/settings.yaml
└─ schema_definition.sql
```
- **Dependencies**:
  - DuckDB
  - PyYAML
- **Used by**: All data source and ingestion modules

### 4. Data Processing
```
ingestion/
├─ importer.py
│  ├─ pandas
│  ├─ duckdb
│  └─ db/duckdb_connection.py
└─ loader.py
   ├─ pandas
   ├─ duckdb
   └─ db/duckdb_connection.py

transformation/
└─ ml_dataset_builder.py
   ├─ pandas
   ├─ numpy
   └─ db/duckdb_connection.py

exporter/
└─ export_dataset.py
   ├─ pandas
   ├─ pyarrow
   └─ db/duckdb_connection.py
```
- **Dependencies**:
  - Pandas
  - DuckDB
  - PyArrow
  - NumPy

## Technical Stack and Dependencies

### Core Dependencies
```txt
duckdb>=0.9.2
requests>=2.31.0
pandas>=2.1.0
pyyaml>=6.0.1
python-dotenv>=1.0.0
pytest>=7.4.0
black>=23.9.1
flake8>=6.1.0
mypy>=1.5.1
```

### Version Compatibility
- Python: 3.8 - 3.11
- DuckDB: 0.9.2+
- Pandas: 2.1.0+
- Requests: 2.31.0+

## Data Flow and Dependencies

1. **Data Collection**
   ```
   Source APIs → Data Source Modules → Configuration
   ```

2. **Data Processing**
   ```
   Data Source Modules → Ingestion Layer → Database → Transformation Layer
   ```

3. **Data Export**
   ```
   Transformation Layer → Export Layer → Output Formats (CSV, Parquet)
   ```

## Development Status

### Completed
- [x] Basic project structure
- [x] CLI menu system
- [x] Database schema definition
- [x] JEPX bid data downloader

### In Progress
- [ ] Implementation of remaining data source modules
- [ ] Database connection management
- [ ] ETL pipeline implementation

### Planned
- [ ] ML dataset builder
- [ ] Data export functionality
- [ ] Comprehensive test suite
- [ ] Documentation

## Security and Performance Considerations

### Security
- API key management through environment variables
- Database credentials through configuration
- HTTPS for all external API calls

### Performance
- DuckDB for efficient data processing
- Pandas for data manipulation
- PyArrow for efficient data export

## Future Enhancements
1. Real-time data streaming
2. Advanced data validation
3. Machine learning model integration
4. Web API interface
5. Data visualization dashboard 