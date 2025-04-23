#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
電力市場データポータル

このスクリプトは、電力市場データの取得・処理・保存に関する
様々な機能へのアクセスを提供するメインポータルです。
全ての主要な機能に統一的なインターフェースからアクセスできます。
"""

import os
import sys
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
import duckdb
import pandas as pd

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# モジュールのインポート
try:
    from cli.menu import Menu
    from data_sources.tso.unified_downloader import UnifiedTSODownloader
    from data_sources.tso.db_importer import TSODataImporter
    from db.duckdb_connection import DuckDBConnection
    from data_sources.jepx.jepx_da_price import JEPXDAPriceDownloader
except ImportError as e:
    logger.error(f"モジュールのインポートエラー: {e}")
    logger.info("プロジェクトのルートディレクトリを Python パスに追加します")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from cli.menu import Menu
    from data_sources.tso.unified_downloader import UnifiedTSODownloader
    from data_sources.tso.db_importer import TSODataImporter
    from db.duckdb_connection import DuckDBConnection
    from data_sources.jepx.jepx_da_price import JEPXDAPriceDownloader

class PowerMarketPortal:
    """電力市場データポータルのメインクラス"""
    
    def __init__(self, db_path: str = None):
        """
        ポータルの初期化
        
        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = db_path or os.getenv("DB_PATH", "/Volumes/MacMiniSSD/powermarketdata/power_market_data")
        print(f"[main.py] PowerMarketPortal: Using DB file: {self.db_path}")
        logger.info(f"[main.py] PowerMarketPortal: Using DB file: {self.db_path}")
        self.db = DuckDBConnection()
    
    def __del__(self):
        """デストラクタ - インスタンスが破棄される際にDB接続を閉じる"""
        if hasattr(self, 'db') and self.db is not None:
            try:
                self.db.close()
                print(f"[INFO] main.py: PowerMarketPortalインスタンス破棄時にデータベース接続を閉じました")
            except Exception as e:
                print(f"[ERROR] PowerMarketPortalのデストラクタでデータベース接続を閉じる際にエラー: {e}")
                
    def close(self):
        """明示的にリソースを解放するメソッド"""
        if hasattr(self, 'db') and self.db is not None:
            try:
                self.db.close()
                self.db = None
                print(f"[INFO] main.py: PowerMarketPortalのclose()メソッドでデータベース接続を閉じました")
            except Exception as e:
                print(f"[ERROR] PowerMarketPortalのclose()メソッドでエラー: {e}")
    
    def download_tso_data(self, start_date: date, end_date: date, tso_ids: Optional[List[str]] = None, url_type: str = "demand") -> int:
        logger.info(f"TSOデータのダウンロード（{start_date}～{end_date}）, url_type={url_type}")
        imported_rows = 0
        importer = None
        try:
            importer = TSODataImporter()
            imported_rows = importer.import_from_downloader(
                tso_ids=tso_ids,
                start_date=start_date,
                end_date=end_date,
                url_type=url_type
            )
            logger.info(f"{imported_rows}行のTSOデータを保存しました")
            return imported_rows
        except Exception as e:
            logger.error(f"TSOデータのダウンロード中にエラーが発生: {e}")
            print(f"[ERROR] TSOデータのダウンロード中にエラーが発生: {e}")
            return 0
        finally:
            # データベース接続をクリーンアップ
            if importer and hasattr(importer, 'db'):
                try:
                    importer.db.close()
                    print(f"[INFO] main.py: TSOインポート後にデータベース接続を閉じました")
                except Exception as close_error:
                    logger.error(f"データベース接続のクローズに失敗: {close_error}")
                    print(f"[ERROR] データベース接続のクローズに失敗: {close_error}")
    
    def download_jepx_price(self, url: Optional[str] = None) -> int:
        """
        JEPX スポット価格をダウンロードしてデータベースに保存
        
        Args:
            url: JEPXのURL（省略時はデフォルト）
            
        Returns:
            インポートされた行数
        """
        logger.info("JEPXスポット価格データのダウンロード")
        downloader = JEPXDAPriceDownloader()
        rows = downloader.fetch_and_store(url)
        logger.info(f"{rows}行のJEPXスポット価格データを保存しました")
        return rows
    
    def interactive_menu(self):
        """インタラクティブメニューを表示"""
        # メニュークラスは内部で独自のPowerMarketPortalを生成するため、
        # 依存性を外すためにシンプルなMenuの呼び出しに変更
        menu = Menu()
        menu.run()

def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="電力市場データポータル")
    
    # 共通のデータベースパスオプション
    parser.add_argument(
        "--db-path",
        type=str,
        default="/Volumes/MacMiniSSD/powermarketdata/power_market_data",
        help="データベースファイルのパス"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="実行するコマンド")
    
    # TSO需要・供給データダウンロード（統合コマンド）
    tso_data_parser = subparsers.add_parser("tso-data", help="TSO需要・供給データをダウンロード")
    tso_data_parser.add_argument(
        "--start-date",
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
        help="開始日（YYYY-MM-DD形式）",
        default=(date.today() - timedelta(days=7)),
    )
    tso_data_parser.add_argument(
        "--end-date",
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
        help="終了日（YYYY-MM-DD形式）",
        default=date.today(),
    )
    tso_data_parser.add_argument(
        "--tso-ids",
        type=str,
        nargs="+",
        help="処理対象のTSO ID（例: tepco hokkaido）指定しない場合は全て",
    )
    
    # JEPXスポット価格ダウンロード
    jepx_price_parser = subparsers.add_parser("jepx-price", help="JEPXスポット価格をダウンロード")
    jepx_price_parser.add_argument(
        "--url",
        type=str,
        help="JEPXスポット価格データのURL（省略時はデフォルト）",
    )
    
    # インタラクティブメニュー
    menu_parser = subparsers.add_parser("menu", help="インタラクティブメニューを表示")
    
    return parser.parse_args()

def main():
    """メイン関数"""
    args = parse_args()
    
    # ポータルインスタンスを作成（データベースパスを指定）
    portal = PowerMarketPortal(db_path=args.db_path if hasattr(args, 'db_path') else "/Volumes/MacMiniSSD/powermarketdata/power_market_data")
    
    try:
        if args.command == "tso-data":
            print(f"Downloading TSO data from {args.start_date} to {args.end_date}...")
            rows = portal.download_tso_data(
                start_date=args.start_date,
                end_date=args.end_date,
                tso_ids=args.tso_ids
            )
            print(f"Successfully imported {rows} rows of TSO data")
        elif args.command == "jepx-price":
            # JEPXスポット価格のダウンロード
            portal.download_jepx_price(url=args.url if hasattr(args, 'url') else None)
        elif args.command == "menu" or not args.command:
            # インタラクティブメニューの表示
            portal.interactive_menu()
        else:
            logger.error(f"不明なコマンド: {args.command}")
            return 1
        
        return 0
    finally:
        # 明示的にデータベース接続を閉じる
        if portal:
            portal.close()
            print("[INFO] main.py: メイン処理完了後にデータベース接続を閉じました")

if __name__ == "__main__":
    sys.exit(main())