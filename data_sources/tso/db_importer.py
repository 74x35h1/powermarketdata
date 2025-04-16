#!/usr/bin/env python3
"""
TSO (送電系統運用者) データDBインポーター

このスクリプトは、ダウンロードしたTSOデータをデータベースに保存します。
UnifiedTSODownloaderから取得したデータフレームをDuckDBに挿入する機能を提供します。
"""

import os
import sys
import argparse
import logging
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple, Union

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.db_connection import DuckDBConnection
from data_sources.tso.unified_downloader import UnifiedTSODownloader
from data_sources.tso.tso_urls import TSO_INFO

# ロギングを設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TSODataImporter:
    """
    TSOデータをデータベースにインポートするクラス
    
    このクラスは、UnifiedTSODownloaderから取得したデータをデータベースに保存する機能を提供します。
    """
    
    def __init__(self, db_path: str = None):
        """
        TSOデータインポーターを初期化
        
        Args:
            db_path: DuckDBデータベースファイルへのパス。指定がなければデフォルトパスが使用されます。
        """
        self.db = DuckDBConnection() if db_path is None else DuckDBConnection(db_path)
        self._ensure_tables()
    
    def _ensure_tables(self):
        """データベーステーブルが存在することを確認"""
        # TSO需要データテーブル
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS tso_demand (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                hour INTEGER,
                time_slot VARCHAR,
                demand_actual DOUBLE,
                demand_forecast DOUBLE,
                tso_id VARCHAR,
                area_code VARCHAR,
                region VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # TSO供給データテーブル
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS tso_supply (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                hour INTEGER,
                time_slot VARCHAR,
                supply_capacity DOUBLE,
                nuclear DOUBLE,
                thermal DOUBLE,
                hydro DOUBLE,
                solar DOUBLE,
                wind DOUBLE,
                pumped_storage DOUBLE,
                biomass DOUBLE,
                geothermal DOUBLE,
                other DOUBLE,
                tso_id VARCHAR,
                area_code VARCHAR,
                region VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # エリア情報テーブル
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS tso_areas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tso_id VARCHAR UNIQUE,
                name VARCHAR,
                area_code VARCHAR,
                region VARCHAR
            )
        """)
        
        # エリア情報を追加（存在しない場合のみ）
        for tso_id, info in TSO_INFO.items():
            self.db.execute_query("""
                INSERT OR IGNORE INTO tso_areas (tso_id, name, area_code, region)
                VALUES (?, ?, ?, ?)
            """, (tso_id, info['name'], info['area_code'], info['region']))
    
    def import_data(self, data_frames: List[Tuple[date, str, pd.DataFrame]], url_type: str) -> int:
        """
        UnifiedTSODownloaderから取得したデータをインポート
        
        Args:
            data_frames: (日付, TSO ID, データフレーム)のタプルのリスト
            url_type: データの種類（'demand'または'supply'）
            
        Returns:
            インポートされた行数の合計
        """
        if not data_frames:
            logger.warning("インポートするデータがありません")
            return 0
        
        table_name = f"tso_{url_type}"
        total_rows = 0
        
        for target_date, tso_id, df in data_frames:
            if df is None or df.empty:
                logger.warning(f"{target_date} の {tso_id} データが空です")
                continue
            
            # TSO情報を追加
            df['area_code'] = TSO_INFO[tso_id]['area_code']
            df['region'] = TSO_INFO[tso_id]['region']
            
            # 不要な列を削除
            cols_to_drop = [col for col in df.columns if col.startswith('Unnamed:')]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
            
            # データフレームをDBに保存
            try:
                # データベースへの保存に必要な列があることを確認
                required_cols = ['date', 'tso_id']
                if not all(col in df.columns for col in required_cols):
                    logger.error(f"必須列が不足しています: {required_cols}")
                    continue
                
                # データフレームをJSON形式でログに出力（デバッグ用）
                logger.debug(f"保存するデータサンプル: {df.head(2).to_json()}")
                
                # NULLやNaN値を適切に処理
                df = df.where(pd.notnull(df), None)
                
                # 日付フォーマットの正規化
                if 'date' in df.columns and not pd.api.types.is_datetime64_dtype(df['date']):
                    df['date'] = pd.to_datetime(df['date']).dt.date
                
                # データフレームをDBに保存
                rows_saved = self.db.save_dataframe(df, table_name)
                logger.info(f"{target_date} の {tso_id} データから {rows_saved} 行を {table_name} に保存しました")
                total_rows += rows_saved
                
            except Exception as e:
                logger.error(f"データ保存エラー: {str(e)}")
                logger.debug(f"エラーの発生した列: {df.columns.tolist()}")
        
        return total_rows
    
    def import_from_downloader(
        self, 
        tso_ids: List[str] = None, 
        start_date: date = None, 
        end_date: date = None,
        url_type: str = 'demand'
    ) -> int:
        """
        指定されたTSOとデータ期間のデータをダウンロードしてインポート
        
        Args:
            tso_ids: インポートするTSO IDのリスト（省略すると全TSO）
            start_date: ダウンロード開始日（省略すると今日）
            end_date: ダウンロード終了日（省略すると開始日）
            url_type: データの種類（'demand'または'supply'）
            
        Returns:
            インポートされた行数の合計
        """
        # 日付のデフォルト値を設定
        if start_date is None:
            start_date = date.today()
        if end_date is None:
            end_date = start_date
        
        # 全TSO IDsをデフォルトとして使用
        if tso_ids is None:
            tso_ids = list(TSO_INFO.keys())
        
        # ダウンローダーを初期化
        downloader = UnifiedTSODownloader(
            tso_ids=tso_ids,
            db_connection=None,  # DB接続はここでは使用しない（バッチで処理するため）
            url_type=url_type
        )
        
        # データをダウンロード
        logger.info(f"{start_date} から {end_date} までの {', '.join(tso_ids)} データをダウンロード中")
        results = downloader.download_files(start_date, end_date)
        
        # ダウンロードしたデータをインポート
        imported_rows = self.import_data(results, url_type)
        
        return imported_rows


def parse_args():
    """コマンドライン引数を解析"""
    parser = argparse.ArgumentParser(description='TSO データをダウンロードしてデータベースにインポートします')
    
    parser.add_argument('--tso-id', dest='tso_ids', action='append',
                        help='インポートするTSO ID（複数回指定可能、省略時は全TSO）')
    
    parser.add_argument('--start-date', dest='start_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='ダウンロード開始日（YYYY-MM-DD形式、省略時は今日）')
    
    parser.add_argument('--end-date', dest='end_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='ダウンロード終了日（YYYY-MM-DD形式、省略時は開始日と同じ）')
    
    parser.add_argument('--url-type', dest='url_type', choices=['demand', 'supply'], default='demand',
                        help='データの種類（demand または supply、デフォルトはdemand）')
    
    parser.add_argument('--db-path', dest='db_path',
                        help='DuckDBデータベースファイルへのパス（省略時はデフォルト）')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='詳細なログ出力を有効にする')
    
    return parser.parse_args()


def main():
    """メイン関数"""
    args = parse_args()
    
    # 詳細ログの設定
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # インポーターを初期化
        importer = TSODataImporter(db_path=args.db_path)
        
        # データをダウンロードしてインポート
        imported_rows = importer.import_from_downloader(
            tso_ids=args.tso_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            url_type=args.url_type
        )
        
        logger.info(f"合計 {imported_rows} 行のデータをインポートしました")
        return 0
        
    except KeyboardInterrupt:
        logger.info("ユーザーによって処理が中断されました")
        return 1
    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 