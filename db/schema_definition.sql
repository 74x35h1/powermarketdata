-- JEPX Spot Market Data
CREATE TABLE IF NOT EXISTS jepx_spot_prices (
    date DATE,
    hour INTEGER,
    price DECIMAL(10,2),
    volume DECIMAL(10,2),
    PRIMARY KEY (date, hour)
);

CREATE TABLE IF NOT EXISTS jepx_bid_curves (
    date DATE,
    hour INTEGER,
    price DECIMAL(10,2),
    volume DECIMAL(10,2),
    PRIMARY KEY (date, hour, price)
);

-- Weather Data
CREATE TABLE IF NOT EXISTS jma_weather (
    date DATE,
    hour INTEGER,
    station_id VARCHAR(10),
    temperature DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    PRIMARY KEY (date, hour, station_id)
);

-- TSO Demand Data
CREATE TABLE IF NOT EXISTS tso_demand (
    date DATE,
    hour INTEGER,
    area VARCHAR(10),
    demand DECIMAL(10,2),
    PRIMARY KEY (date, hour, area)
);

-- OCCTO Data
CREATE TABLE IF NOT EXISTS occto_interconnection (
    date DATE,
    hour INTEGER,
    line_id VARCHAR(10),
    forecast DECIMAL(10,2),
    actual DECIMAL(10,2),
    PRIMARY KEY (date, hour, line_id)
);

CREATE TABLE IF NOT EXISTS occto_reserve (
    date DATE,
    hour INTEGER,
    area VARCHAR(10),
    reserve_rate DECIMAL(5,2),
    PRIMARY KEY (date, hour, area)
);

-- Futures Data
CREATE TABLE IF NOT EXISTS futures_prices (
    date DATE,
    contract VARCHAR(20),
    price DECIMAL(10,2),
    volume INTEGER,
    exchange VARCHAR(10),
    PRIMARY KEY (date, contract, exchange)
); 