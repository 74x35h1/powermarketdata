#!/usr/bin/env python3
"""
TSO (送電系統運用者) データのダウンロードとDB保存の例

このスクリプトは、日本の電力会社（TSO）からデータをダウンロードし、
DuckDBデータベースに保存する方法を示します。
"""

import os
import sys
from datetime import date, timedelta
import duckdb

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.tso.db_importer import TSODataImporter

def main():
    """TSO データをダウンロードしてDBに保存する例"""
    db_path = "/Volumes/MacMiniSSD/powermarketdata/power_market_data"
    importer = TSODataImporter()

    # 直近1週間分のデータをダウンロード
    end_date = date.today()
    start_date = end_date - timedelta(days=6)
    print(f"TSOデータを {start_date} から {end_date} までダウンロードして保存します...")
    rows = importer.import_from_downloader(start_date=start_date, end_date=end_date)
    print(f"{rows} 行のTSOデータをtso_dataテーブルに保存しました。")

    print("\nTSOデータサンプル (tso_data):")
    con = duckdb.connect(db_path)
    result = con.execute("""
        SELECT * FROM tso_data
        ORDER BY date DESC, slot
        LIMIT 5
    """)
    rows = result.fetchall()
    for row in rows:
        print(row)
    con.close()
    print("\n処理が完了しました。")

if __name__ == "__main__":
    main() 