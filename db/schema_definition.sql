-- JEPX Spot Market Data
CREATE TABLE IF NOT EXISTS jepx_da_price (
    date TEXT,
    slot INTEGER,
    sell_bid_qty_kwh INTEGER,
    buy_bid_qty_kwh INTEGER,
    contract_qty_kwh INTEGER,
    ap0_system DECIMAL(5,2),
    ap1_hokkaido DECIMAL(5,2),
    ap2_tohoku DECIMAL(5,2),
    ap3_tokyo DECIMAL(5,2),
    ap4_chubu DECIMAL(5,2),
    ap5_hokuriku DECIMAL(5,2),
    ap6_kansai DECIMAL(5,2),
    ap7_chugoku DECIMAL(5,2),
    ap8_shikoku DECIMAL(5,2),
    ap9_kyushu DECIMAL(5,2),
    spot_avg_price DECIMAL(5,2),
    alpha_upper_spot_avg_price DECIMAL(5,2),
    alpha_lower_spot_avg_price DECIMAL(5,2),
    alpha_flash_spot_avg_price DECIMAL(5,2),
    alpha_confirmed_spot_avg_price DECIMAL(5,2),
    avoidable_cost_national DECIMAL(5,2),
    avoidable_cost_hokkaido DECIMAL(5,2),
    avoidable_cost_tohoku DECIMAL(5,2),
    avoidable_cost_tokyo DECIMAL(5,2),
    avoidable_cost_chubu DECIMAL(5,2),
    avoidable_cost_hokuriku DECIMAL(5,2),
    avoidable_cost_kansai DECIMAL(5,2),
    avoidable_cost_chugoku DECIMAL(5,2),
    avoidable_cost_shikoku DECIMAL(5,2),
    avoidable_cost_kyushu DECIMAL(5,2),
    sell_block_bid_qty_kwh INTEGER,
    sell_block_contract_qty_kwh INTEGER,
    buy_block_bid_qty_kwh INTEGER,
    buy_block_contract_qty_kwh INTEGER,    
    fip_ref_price_national DECIMAL(5,2),
    fip_ref_price_hokkaido DECIMAL(5,2),
    fip_ref_price_tohoku DECIMAL(5,2),
    fip_ref_price_tokyo DECIMAL(5,2),
    fip_ref_price_chubu DECIMAL(5,2),
    fip_ref_price_hokuriku DECIMAL(5,2),
    fip_ref_price_kansai DECIMAL(5,2),
    fip_ref_price_chugoku DECIMAL(5,2),
    fip_ref_price_shikoku DECIMAL(5,2),
    fip_ref_price_kyushu DECIMAL(5,2),
    PRIMARY KEY (date, slot)
);

CREATE TABLE IF NOT EXISTS jepx_bid_curves (
    id VARCHAR(20) PRIMARY KEY,  -- 20250401_48_1
    date CHAR(8) NOT NULL,       -- YYYYMMDD
    slot INT NOT NULL,           -- 時間帯
    area_code INT NOT NULL,      -- エリアコード
    bid JSON NOT NULL            -- JSON配列形式: 
);

-- Weather Data
CREATE TABLE IF NOT EXISTS jma_weather (
    station_id VARCHAR(10) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    interval VARCHAR(10) NOT NULL,
    temperature DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    sunshine_duration DECIMAL(5,2),
    wind_speed DECIMAL(5,2),
    wind_direction VARCHAR(10),
    wind_direction_sin REAL,
    wind_direction_cos REAL,
    PRIMARY KEY (station_id, datetime, interval)
);

-- エリア情報テーブル
CREATE TABLE IF NOT EXISTS tso_areas (
    id INTEGER PRIMARY KEY,
    tso_id VARCHAR UNIQUE,
    name VARCHAR,
    area_code VARCHAR,
    region VARCHAR
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

CREATE TABLE IF NOT EXISTS occto_plant_operation (
    date DATE,
    time VARCHAR,
    area VARCHAR,
    plant_name VARCHAR,
    plant_type VARCHAR,
    output_kw DECIMAL(18,6),
    processing_date DATE,
    PRIMARY KEY (date, time, plant_name)
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

-- TSOエリア別データテーブル（1～9）
CREATE TABLE IF NOT EXISTS tso_area_1_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_2_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_3_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_4_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_5_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_6_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_7_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_8_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);
CREATE TABLE IF NOT EXISTS tso_area_9_data (
    master_key VARCHAR PRIMARY KEY,
    date TEXT,
    slot INTEGER,
    area_demand DOUBLE,
    nuclear DOUBLE,
    LNG DOUBLE,
    coal DOUBLE,
    oil DOUBLE,
    other_fire DOUBLE,
    hydro DOUBLE,
    geothermal DOUBLE,
    biomass DOUBLE,
    solar_actual DOUBLE,
    solar_control DOUBLE,
    wind_actual DOUBLE,
    wind_control DOUBLE,
    pumped_storage DOUBLE,
    battery DOUBLE,
    interconnection DOUBLE,
    other DOUBLE,
    total DOUBLE
);

-- OCCTO 30-minute Generation Data
CREATE TABLE IF NOT EXISTS occto_30min_generation (
    master_key TEXT PRIMARY KEY, -- Combined key: YYYYMMDD_plantcode_unitnum
    date TEXT NOT NULL,          -- YYYYMMDD format
    plant_code TEXT NOT NULL,
    unit_num TEXT NOT NULL,
    area_code INTEGER,
    plant_name TEXT,
    gen_method TEXT,
    slot1 INTEGER, slot2 INTEGER, slot3 INTEGER, slot4 INTEGER, slot5 INTEGER, slot6 INTEGER,
    slot7 INTEGER, slot8 INTEGER, slot9 INTEGER, slot10 INTEGER, slot11 INTEGER, slot12 INTEGER,
    slot13 INTEGER, slot14 INTEGER, slot15 INTEGER, slot16 INTEGER, slot17 INTEGER, slot18 INTEGER,
    slot19 INTEGER, slot20 INTEGER, slot21 INTEGER, slot22 INTEGER, slot23 INTEGER, slot24 INTEGER,
    slot25 INTEGER, slot26 INTEGER, slot27 INTEGER, slot28 INTEGER, slot29 INTEGER, slot30 INTEGER,
    slot31 INTEGER, slot32 INTEGER, slot33 INTEGER, slot34 INTEGER, slot35 INTEGER, slot36 INTEGER,
    slot37 INTEGER, slot38 INTEGER, slot39 INTEGER, slot40 INTEGER, slot41 INTEGER, slot42 INTEGER,
    slot43 INTEGER, slot44 INTEGER, slot45 INTEGER, slot46 INTEGER, slot47 INTEGER, slot48 INTEGER,
    total INTEGER
    -- PRIMARY KEY (date, plant_code, unit_num) -- Replaced by master_key
); 