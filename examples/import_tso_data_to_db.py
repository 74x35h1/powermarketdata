#!/usr/bin/env python3
"""
TSO (送電系統運用者) データのダウンロードとDB保存の例

このスクリプトは、日本の電力会社（TSO）からデータをダウンロードし、
DuckDBデータベースに保存する方法を示します。
"""

import os
import sys
from datetime import date, timedelta

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.tso.db_importer import TSODataImporter

def main():
    """TSO データをダウンロードしてDBに保存する例"""
    
    # 対象期間を設定（直近の1ヶ月）
    end_date = date.today()
    start_date = end_date.replace(day=1)  # 今月の初日
    
    print(f"\n期間: {start_date} から {end_date}")
    
    # 例1: すべてのTSOの需要データをダウンロードしてDBに保存
    print("\n例1: すべてのTSOの需要データをインポート")
    print("=" * 70)
    
    importer = TSODataImporter()
    rows_imported = importer.import_from_downloader(
        start_date=start_date,
        end_date=end_date,
        url_type='demand'
    )
    
    print(f"需要データ: {rows_imported} 行をインポートしました")
    
    # 例2: 特定のTSO（東京電力と関西電力）の供給データをダウンロードしてDBに保存
    print("\n例2: 特定TSO（東京電力と関西電力）の供給データをインポート")
    print("=" * 70)
    
    importer = TSODataImporter()
    rows_imported = importer.import_from_downloader(
        tso_ids=['tepco', 'kepco'],
        start_date=start_date,
        end_date=end_date,
        url_type='supply'
    )
    
    print(f"供給データ: {rows_imported} 行をインポートしました")
    
    # 保存したデータの確認
    print("\nデータベースに保存されたデータの確認:")
    print("=" * 70)
    
    # 需要データの確認
    print("\n需要データサンプル (tso_demand):")
    result = importer.db.execute_query("""
        SELECT date, hour, tso_id, area_code, demand_actual, demand_forecast
        FROM tso_demand
        ORDER BY date DESC, tso_id, hour
        LIMIT 5
    """)
    if result:
        rows = result.fetchall()
        for row in rows:
            print(f"  {row[0]} {row[1]:02d}時 {row[2]} (エリア{row[3]}): 需要 {row[4]:.2f} kW")
    
    # 供給データの確認
    print("\n供給データサンプル (tso_supply):")
    result = importer.db.execute_query("""
        SELECT date, hour, tso_id, supply_capacity, nuclear, thermal, solar, wind
        FROM tso_supply
        ORDER BY date DESC, tso_id, hour
        LIMIT 5
    """)
    if result:
        rows = result.fetchall()
        for row in rows:
            print(f"  {row[0]} {row[1]:02d}時 {row[2]}: 供給力 {row[3]:.2f} kW (原子力: {row[4]:.2f}, 火力: {row[5]:.2f}, 太陽光: {row[6]:.2f}, 風力: {row[7]:.2f})")
    
    print("\n処理が完了しました。")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 