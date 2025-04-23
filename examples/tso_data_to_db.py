#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TSO (電力広域的運営推進機関) データを全エリア横持ちでDBに格納するスクリプト

- 1行=1スロット（北海道エリアのDATE+TIMEスロットがマスターキー）
- 各エリア（1:北海道, ... 9:九州）の各項目を横持ちで格納
- テーブル名: tso_data
"""
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_sources.tso.db_importer import TSODataImporter
from data_sources.tso.unified_downloader import UnifiedTSODownloader
from db.duckdb_connection import DuckDBConnection
from data_sources.tso.tso_urls import TSO_INFO

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# エリア番号とTSO IDの対応
AREA_ORDER = [
    (1, 'hokkaido'),
    (2, 'tohoku'),
    (3, 'tepco'),
    (4, 'chubu'),
    (5, 'hokuriku'),
    (6, 'kepco'),
    (7, 'chugoku'),
    (8, 'shikoku'),
    (9, 'kyushu'),
]

# 項目名の英語変換
ITEMS = [
    ('エリア需要', 'area_demand'),
    ('原子力', 'nuclear'),
    ('火力(LNG)', 'LNG'),
    ('火力(石炭)', 'coal'),
    ('火力(石油)', 'oil'),
    ('火力(その他)', 'other_fire'),
    ('水力', 'hydro'),
    ('地熱', 'geothermal'),
    ('バイオマス', 'biomass'),
    ('太陽光発電実績', 'solar_actual'),
    ('太陽光出力制御量', 'solar_control'),
    ('風力発電実績', 'wind_actual'),
    ('風力出力制御量', 'wind_control'),
    ('揚水', 'pumped_storage'),
    ('蓄電池', 'battery'),
    ('連系線', 'interconnection'),
    ('その他', 'other'),
    ('合計', 'total'),
]

def parse_args():
    parser = argparse.ArgumentParser(description="Download TSO data and store in wide-format table")
    parser.add_argument('--start-date', type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(), required=True)
    parser.add_argument('--end-date', type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(), required=True)
    parser.add_argument('--log-level', type=str, default="INFO")
    return parser.parse_args()

def time_to_slot(time_str: str) -> int:
    # "00:00"→1, "23:30"→48
    h, m = map(int, time_str.split(":"))
    return h * 2 + (1 if m == 0 else 2)

def debug_table_info(db: DuckDBConnection, table: str):
    try:
        cols = db.execute_query(f"PRAGMA table_info({table})").fetchall()
        print(f"[DEBUG] Columns in {table}: {[c[1] for c in cols]}")
        cnt = db.execute_query(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[DEBUG] Row count in {table}: {cnt}")
    except Exception as e:
        print(f"[DEBUG] Table {table} does not exist or error: {e}")

def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    try:
        # スキーマ定義から全テーブルを作成
        importer = TSODataImporter()
        db = DuckDBConnection()
        print("[DEBUG] After ensure_tables() call:")
        debug_table_info(db, 'tso_data')

        all_area_dfs: Dict[str, pd.DataFrame] = {}
        for area_num, tso_id in AREA_ORDER:
            logger.info(f"Downloading data for area {area_num}: {tso_id}")
            downloader = UnifiedTSODownloader(tso_ids=[tso_id], url_type='demand')
            results = downloader.download_files(args.start_date, args.end_date)
            dfs = [df for _, _, df in results if df is not None and not df.empty]
            if not dfs:
                logger.warning(f"No data for {tso_id}")
                continue
            df = pd.concat(dfs, ignore_index=True)
            col_map = {jp: en for jp, en in ITEMS}
            select_cols = ['date', 'time_slot', *col_map.keys()]
            df = df[[c for c in select_cols if c in df.columns]].copy()
            df.rename(columns=col_map, inplace=True)
            df['slot'] = df['time_slot'].apply(time_to_slot)
            df['area_num'] = area_num
            all_area_dfs[tso_id] = df

        hokkaido_df = all_area_dfs.get('hokkaido')
        if hokkaido_df is None:
            logger.error("No data for Hokkaido area. Cannot proceed.")
            sys.exit(1)
        wide_rows = []
        for idx, row in hokkaido_df.iterrows():
            master_key = f"{row['date'].strftime('%Y%m%d')}_{row['slot']:02d}"
            base = {
                'master_key': master_key,
                'date': row['date'],
                'slot': row['slot'],
            }
            for area_num, tso_id in AREA_ORDER:
                area_row = all_area_dfs[tso_id][
                    (all_area_dfs[tso_id]['date'] == row['date']) & (all_area_dfs[tso_id]['slot'] == row['slot'])
                ]
                if not area_row.empty:
                    area_row = area_row.iloc[0]
                    for _, en in ITEMS:
                        base[f'{area_num}_{en}'] = area_row.get(en, None)
                else:
                    for _, en in ITEMS:
                        base[f'{area_num}_{en}'] = None
            wide_rows.append(base)
        wide_df = pd.DataFrame(wide_rows)
        print(f"[DEBUG] wide_df shape: {wide_df.shape}")
        print(f"[DEBUG] wide_df columns: {list(wide_df.columns)}")
        print(f"[DEBUG] wide_df head:\n{wide_df.head()}")
        logger.info(f"Saving {len(wide_df)} rows to tso_data table...")
        db.save_dataframe(wide_df, 'tso_data')
        print("[DEBUG] After save_dataframe:")
        debug_table_info(db, 'tso_data')
        logger.info("Done.")
    except Exception as e:
        logger.error(f"Error: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main() 