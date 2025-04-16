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