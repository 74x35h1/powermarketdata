import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import requests
import csv
import io
from decimal import Decimal, InvalidOperation
import chardet
from db.duckdb_connection import DuckDBConnection

JEPX_DA_PRICE_URL = "https://www.jepx.jp/market/excel/spot_2025.csv"

# schema_definition.sqlに合わせた日本語→英語カラム名変換辞書
JP2EN = {
    '年月日': 'date',
    '時刻コード': 'slot',
    '売り入札量(kWh)': 'sell_bid_qty_kwh',
    '買い入札量(kWh)': 'buy_bid_qty_kwh',
    '約定総量(kWh)': 'contract_qty_kwh',
    'システムプライス(円/kWh)': 'ap0_system',
    'エリアプライス北海道(円/kWh)': 'ap1_hokkaido',
    'エリアプライス東北(円/kWh)': 'ap2_tohoku',
    'エリアプライス東京(円/kWh)': 'ap3_tokyo',
    'エリアプライス中部(円/kWh)': 'ap4_chubu',
    'エリアプライス北陸(円/kWh)': 'ap5_hokuriku',
    'エリアプライス関西(円/kWh)': 'ap6_kansai',
    'エリアプライス中国(円/kWh)': 'ap7_chugoku',
    'エリアプライス四国(円/kWh)': 'ap8_shikoku',
    'エリアプライス九州(円/kWh)': 'ap9_kyushu',
    'スポット・時間前平均価格(円/kWh)': 'spot_avg_price',
    'α上限値×スポット・時間前平均価格(円/kWh)': 'alpha_upper_spot_avg_price',
    'α下限値×スポット・時間前平均価格(円/kWh)': 'alpha_lower_spot_avg_price',
    'α速報値×スポット・時間前平均価格(円/kWh)': 'alpha_flash_spot_avg_price',
    'α確報値×スポット・時間前平均価格(円/kWh)': 'alpha_confirmed_spot_avg_price',
    '回避可能原価全国値(円/kWh)': 'avoidable_cost_national',
    '回避可能原価北海道(円/kWh)': 'avoidable_cost_hokkaido',
    '回避可能原価東北(円/kWh)': 'avoidable_cost_tohoku',
    '回避可能原価東京(円/kWh)': 'avoidable_cost_tokyo',
    '回避可能原価中部(円/kWh)': 'avoidable_cost_chubu',
    '回避可能原価北陸(円/kWh)': 'avoidable_cost_hokuriku',
    '回避可能原価関西(円/kWh)': 'avoidable_cost_kansai',
    '回避可能原価中国(円/kWh)': 'avoidable_cost_chugoku',
    '回避可能原価四国(円/kWh)': 'avoidable_cost_shikoku',
    '回避可能原価九州(円/kWh)': 'avoidable_cost_kyushu',
    '売りブロック入札総量(kWh)': 'sell_block_bid_qty_kwh',
    '売りブロック約定総量(kWh)': 'sell_block_contract_qty_kwh',
    '買いブロック入札総量(kWh)': 'buy_block_bid_qty_kwh',
    '買いブロック約定総量(kWh)': 'buy_block_contract_qty_kwh',
    'FIP参照価格（卸電力取引市場分）全国値(円/kWh)': 'fip_ref_price_national',
    'FIP参照価格（卸電力取引市場分）北海道(円/kWh)': 'fip_ref_price_hokkaido',
    'FIP参照価格（卸電力取引市場分）東北(円/kWh)': 'fip_ref_price_tohoku',
    'FIP参照価格（卸電力取引市場分）東京(円/kWh)': 'fip_ref_price_tokyo',
    'FIP参照価格（卸電力取引市場分）中部(円/kWh)': 'fip_ref_price_chubu',
    'FIP参照価格（卸電力取引市場分）北陸(円/kWh)': 'fip_ref_price_hokuriku',
    'FIP参照価格（卸電力取引市場分）関西(円/kWh)': 'fip_ref_price_kansai',
    'FIP参照価格（卸電力取引市場分）中国(円/kWh)': 'fip_ref_price_chugoku',
    'FIP参照価格（卸電力取引市場分）四国(円/kWh)': 'fip_ref_price_shikoku',
    'FIP参照価格（卸電力取引市場分）九州(円/kWh)': 'fip_ref_price_kyushu',
}

# schema_definition.sqlのカラム順（英語名）
SCHEMA_COLS = [
    'date', 'slot',
    'sell_bid_qty_kwh', 'buy_bid_qty_kwh', 'contract_qty_kwh',
    'ap0_system', 'ap1_hokkaido', 'ap2_tohoku', 'ap3_tokyo', 'ap4_chubu', 'ap5_hokuriku', 'ap6_kansai', 'ap7_chugoku', 'ap8_shikoku', 'ap9_kyushu',
    'spot_avg_price', 'alpha_upper_spot_avg_price', 'alpha_lower_spot_avg_price', 'alpha_flash_spot_avg_price', 'alpha_confirmed_spot_avg_price',
    'avoidable_cost_national', 'avoidable_cost_hokkaido', 'avoidable_cost_tohoku', 'avoidable_cost_tokyo', 'avoidable_cost_chubu', 'avoidable_cost_hokuriku', 'avoidable_cost_kansai', 'avoidable_cost_chugoku', 'avoidable_cost_shikoku', 'avoidable_cost_kyushu',
    'sell_block_bid_qty_kwh', 'sell_block_contract_qty_kwh', 'buy_block_bid_qty_kwh', 'buy_block_contract_qty_kwh',
    'fip_ref_price_national', 'fip_ref_price_hokkaido', 'fip_ref_price_tohoku', 'fip_ref_price_tokyo', 'fip_ref_price_chubu', 'fip_ref_price_hokuriku', 'fip_ref_price_kansai', 'fip_ref_price_chugoku', 'fip_ref_price_shikoku', 'fip_ref_price_kyushu'
]

# カラム型変換（英語名→型）
INT_COLS = {
    'slot', 'sell_bid_qty_kwh', 'buy_bid_qty_kwh', 'contract_qty_kwh',
    'sell_block_bid_qty_kwh', 'sell_block_contract_qty_kwh', 'buy_block_bid_qty_kwh', 'buy_block_contract_qty_kwh'
}
DEC_COLS = {
    'ap0_system', 'ap1_hokkaido', 'ap2_tohoku', 'ap3_tokyo', 'ap4_chubu', 'ap5_hokuriku', 'ap6_kansai', 'ap7_chugoku', 'ap8_shikoku', 'ap9_kyushu',
    'spot_avg_price', 'alpha_upper_spot_avg_price', 'alpha_lower_spot_avg_price', 'alpha_flash_spot_avg_price', 'alpha_confirmed_spot_avg_price',
    'avoidable_cost_national', 'avoidable_cost_hokkaido', 'avoidable_cost_tohoku', 'avoidable_cost_tokyo', 'avoidable_cost_chubu', 'avoidable_cost_hokuriku', 'avoidable_cost_kansai', 'avoidable_cost_chugoku', 'avoidable_cost_shikoku', 'avoidable_cost_kyushu',
    'fip_ref_price_national', 'fip_ref_price_hokkaido', 'fip_ref_price_tohoku', 'fip_ref_price_tokyo', 'fip_ref_price_chubu', 'fip_ref_price_hokuriku', 'fip_ref_price_kansai', 'fip_ref_price_chugoku', 'fip_ref_price_shikoku', 'fip_ref_price_kyushu'
}

class JEPXDAPriceDownloader:
    """
    JEPXスポット価格データダウンローダー
    
    with文で使用することで、ブロックを抜けた時に自動的にデータベース接続を閉じます。
    """
    def __init__(self, db_path: str = None, read_only: bool = False):
        """
        初期化
        
        Args:
            db_path: データベースファイルのパス（省略時はデフォルト）
            read_only: 読み取り専用モードで接続する場合はTrue
        """
        self.db = DuckDBConnection(db_path, read_only=read_only)
        self._ensure_table()
    
    def __enter__(self):
        """コンテキストマネージャのエントリポイント"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャの終了処理"""
        # 明示的にDB接続をクローズ
        try:
            if hasattr(self, 'db') and self.db is not None:
                self.db.close()
        except Exception as e:
            print(f"[WARN] JEPXDAPriceDownloaderのコンテキスト終了時のDB接続クローズでエラー: {e}")

    def _ensure_table(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "db", "schema_definition.sql")
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        stmts = [stmt.strip() for stmt in schema_sql.split(';') if 'jepx_da_price' in stmt]
        for stmt in stmts:
            if stmt:
                self.db.execute_query(stmt)

    def fetch_and_store(self, url=JEPX_DA_PRICE_URL):
        response = requests.get(url)
        raw_bytes = response.content
        
        # エンコーディングはshift_jisに固定
        try:
            csv_text = raw_bytes.decode('shift_jis')
        except Exception as e:
            print(f"デコードエラー: {e}")
            csv_text = raw_bytes.decode('shift_jis', errors='replace')
            
        # 最初の数行をデバッグ出力
        print("First 200 chars of CSV content:")
        print(csv_text[:200])
        
        # CSVファイルとして保存してみる
        debug_csv_path = "debug_jepx_spot.csv"
        with open(debug_csv_path, "wb") as f:
            f.write(raw_bytes)
        print(f"Saved raw CSV to {debug_csv_path}")
        
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        
        if not rows:
            print("WARNING: No rows found in CSV")
            return
            
        # CSVの列インデックスを直接マッピング（JEPXのCSV形式に合わせて）
        # 実際のCSVファイルの順序に基づいて手動でマッピング
        column_mapping = {
            0: 'date',               # 年月日
            1: 'slot',               # 時刻コード
            2: 'sell_bid_qty_kwh',   # 売り入札量(kWh)
            3: 'buy_bid_qty_kwh',    # 買い入札量(kWh)
            4: 'contract_qty_kwh',   # 約定総量(kWh)
            5: 'ap0_system',         # システムプライス(円/kWh)
            6: 'ap1_hokkaido',       # エリアプライス北海道(円/kWh)
            7: 'ap2_tohoku',         # エリアプライス東北(円/kWh)
            8: 'ap3_tokyo',          # エリアプライス東京(円/kWh)
            9: 'ap4_chubu',          # エリアプライス中部(円/kWh)
            10: 'ap5_hokuriku',      # エリアプライス北陸(円/kWh)
            11: 'ap6_kansai',        # エリアプライス関西(円/kWh)
            12: 'ap7_chugoku',       # エリアプライス中国(円/kWh)
            13: 'ap8_shikoku',       # エリアプライス四国(円/kWh)
            14: 'ap9_kyushu',        # エリアプライス九州(円/kWh)
            16: 'spot_avg_price',    # スポット・時間前平均価格(円/kWh)
            17: 'alpha_upper_spot_avg_price',  # α上限値×スポット・時間前平均価格(円/kWh)
            18: 'alpha_lower_spot_avg_price',  # α下限値×スポット・時間前平均価格(円/kWh)
            19: 'alpha_flash_spot_avg_price',  # α速報値×スポット・時間前平均価格(円/kWh)
            20: 'alpha_confirmed_spot_avg_price', # α確報値×スポット・時間前平均価格(円/kWh)
            22: 'avoidable_cost_national',     # 回避可能原価全国値(円/kWh)
            23: 'avoidable_cost_hokkaido',     # 回避可能原価北海道(円/kWh)
            24: 'avoidable_cost_tohoku',       # 回避可能原価東北(円/kWh)
            25: 'avoidable_cost_tokyo',        # 回避可能原価東京(円/kWh)
            26: 'avoidable_cost_chubu',        # 回避可能原価中部(円/kWh)
            27: 'avoidable_cost_hokuriku',     # 回避可能原価北陸(円/kWh)
            28: 'avoidable_cost_kansai',       # 回避可能原価関西(円/kWh)
            29: 'avoidable_cost_chugoku',      # 回避可能原価中国(円/kWh)
            30: 'avoidable_cost_shikoku',      # 回避可能原価四国(円/kWh)
            31: 'avoidable_cost_kyushu',       # 回避可能原価九州(円/kWh)
            33: 'sell_block_bid_qty_kwh',      # 売りブロック入札総量(kWh)
            34: 'sell_block_contract_qty_kwh', # 売りブロック約定総量(kWh)
            35: 'buy_block_bid_qty_kwh',       # 買いブロック入札総量(kWh)
            36: 'buy_block_contract_qty_kwh',  # 買いブロック約定総量(kWh)
            38: 'fip_ref_price_national',      # FIP参照価格（卸電力取引市場分）全国値(円/kWh)
            39: 'fip_ref_price_hokkaido',      # FIP参照価格（卸電力取引市場分）北海道(円/kWh)
            40: 'fip_ref_price_tohoku',        # FIP参照価格（卸電力取引市場分）東北(円/kWh)
            41: 'fip_ref_price_tokyo',         # FIP参照価格（卸電力取引市場分）東京(円/kWh)
            42: 'fip_ref_price_chubu',         # FIP参照価格（卸電力取引市場分）中部(円/kWh)
            43: 'fip_ref_price_hokuriku',      # FIP参照価格（卸電力取引市場分）北陸(円/kWh)
            44: 'fip_ref_price_kansai',        # FIP参照価格（卸電力取引市場分）関西(円/kWh)
            45: 'fip_ref_price_chugoku',       # FIP参照価格（卸電力取引市場分）中国(円/kWh)
            46: 'fip_ref_price_shikoku',       # FIP参照価格（卸電力取引市場分）四国(円/kWh)
            47: 'fip_ref_price_kyushu',        # FIP参照価格（卸電力取引市場分）九州(円/kWh)
        }
        
        print(f"Using direct column mapping: {column_mapping}")
        
        # スキーマカラムに対応するインデックスを生成
        col_idx_map = []
        for col in SCHEMA_COLS:
            idx = next((k for k, v in column_mapping.items() if v == col), None)
            col_idx_map.append(idx)
            if idx is None:
                print(f"WARNING: Column {col} not mapped to any CSV index")
                
        print(f"First 10 schema cols: {SCHEMA_COLS[:10]}")
        print(f"First 10 col_idx_map: {col_idx_map[:10]}")
        
        # 最初の行をサンプルとしてデバッグ出力
        if len(rows) > 1:
            sample_row = rows[1]
            print(f"Sample row length: {len(sample_row)}")
            print(f"Sample row first 15 values: {sample_row[:15]}")
            
            # 値の取得テスト
            for j, col in enumerate(SCHEMA_COLS[:10]):  # 最初の10カラムだけ確認
                idx = col_idx_map[j]
                if idx is not None and idx < len(sample_row):
                    print(f"Column {col} (idx={idx}): {sample_row[idx]}")
                else:
                    print(f"Column {col} has invalid index {idx}")
                
        for row in rows[1:]:
            if not row or len(row) < 2:
                continue
            date = str(row[0]).strip() if len(row) > 0 else None
            slot = str(row[1]).strip() if len(row) > 1 else None
            if not date or not slot:
                print(f"SKIP: date or slot is empty. row={row}")
                continue
            try:
                slot_int = int(slot)
            except Exception:
                print(f"SKIP: slot is not integer. row={row}")
                continue
                
            values = [date, slot_int]
            for j, col in enumerate(SCHEMA_COLS[2:]):
                idx = col_idx_map[j+2]
                val = row[idx] if idx is not None and idx < len(row) else None
                
                # デバッグ出力を削減
                if j < 3 and slot_int <= 3:  # 最初の数カラムを最初の数行だけ表示
                    print(f"Column {col}: idx={idx}, val={val}")
                    
                if col in INT_COLS:
                    try:
                        val = int(val.replace(',', '')) if val else None
                    except Exception:
                        val = None
                elif col in DEC_COLS:
                    try:
                        val = float(val) if val else None
                    except Exception:
                        val = None
                values.append(val)
            
            # デバッグ出力を削減
            if len(values) > 5 and slot_int <= 3:  # 最初の数行だけ表示
                print(f"First 5 values: {values[:5]}")
                
            select_sql = 'SELECT 1 FROM jepx_da_price WHERE date=? AND slot=?'
            exists = self.db.execute_query(select_sql, (date, slot_int)).fetchone()
            if exists:
                set_clause = ','.join([f'{col}=?' for col in SCHEMA_COLS if col not in ['date', 'slot']])
                update_sql = f'UPDATE jepx_da_price SET {set_clause} WHERE date=? AND slot=?'
                update_values = [values[i] for i, col in enumerate(SCHEMA_COLS) if col not in ['date', 'slot']] + [date, slot_int]
                self.db.execute_query(update_sql, tuple(update_values))
            else:
                insert_sql = f'INSERT INTO jepx_da_price ({','.join(SCHEMA_COLS)}) VALUES ({','.join(['?' for _ in SCHEMA_COLS])})'
                self.db.execute_query(insert_sql, tuple(values))
        print('JEPX day-ahead price data updated.')

if __name__ == "__main__":
    with JEPXDAPriceDownloader() as downloader:
        downloader.fetch_and_store()
