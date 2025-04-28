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
    from data_sources.occto.db_importer import OCCTODataImporter
except ImportError as e:
    logger.error(f"モジュールのインポートエラー: {e}")
    logger.info("プロジェクトのルートディレクトリを Python パスに追加します")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from cli.menu import Menu
    from data_sources.tso.unified_downloader import UnifiedTSODownloader
    from data_sources.tso.db_importer import TSODataImporter
    from db.duckdb_connection import DuckDBConnection
    from data_sources.jepx.jepx_da_price import JEPXDAPriceDownloader
    from data_sources.occto.db_importer import OCCTODataImporter

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
        # db接続はメソッド内で with 文を使って行うため、ここでは初期化しない
        self.db = None
    
    def __del__(self):
        """デストラクタ - インスタンスが破棄される際にDB接続を閉じる"""
        # with文使用に変更したため、close不要
        pass
                
    def close(self):
        """明示的にリソースを解放するメソッド"""
        # with文使用に変更したため、close不要
        pass
    
    def download_tso_data(self, start_date: date, end_date: date, tso_ids: List[str], url_type: str = "demand") -> int:
        """
        指定した電力会社エリアのデータをダウンロードする
        
        Args:
            start_date: 開始日
            end_date: 終了日
            tso_ids: TSOエリアIDのリスト（必須）
            url_type: データタイプ（"demand"または"supply"）
            
        Returns:
            インポートされた行数
        """
        if not tso_ids:
            logger.error("TSOエリアIDが指定されていません。少なくとも1つのエリアを指定してください。")
            print("[ERROR] TSOエリアIDが指定されていません。少なくとも1つのエリアを指定してください。")
            return 0
            
        logger.info(f"TSOデータのダウンロード（{start_date}～{end_date}）, url_type={url_type}, tso_ids={tso_ids}")
        imported_rows = 0
        
        try:
            with TSODataImporter() as importer:
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
    
    def download_jepx_price(self, url: Optional[str] = None) -> int:
        """
        JEPX スポット価格をダウンロードしてデータベースに保存
        
        Args:
            url: JEPXのURL（省略時はデフォルト）
            
        Returns:
            インポートされた行数
        """
        logger.info("JEPXスポット価格データのダウンロード")
        
        with JEPXDAPriceDownloader() as downloader:
            rows = downloader.fetch_and_store(url)
            
        logger.info(f"{rows}行のJEPXスポット価格データを保存しました")
        return rows
    
    def download_occto_plant_data(self, start_date: date, end_date: date) -> int:
        """
        OCCTO発電所運転実績データをダウンロードしてデータベースに保存
        
        Args:
            start_date: 開始日
            end_date: 終了日
            
        Returns:
            インポートされた行数
        """
        logger.info(f"OCCTO発電所運転実績データのダウンロード ({start_date}～{end_date})")
        
        try:
            with OCCTODataImporter(db_path=self.db_path) as importer:
                imported_rows = importer.import_plant_operation_data(
                    start_date=start_date,
                    end_date=end_date,
                    save_to_db=True
                )
            
            logger.info(f"{imported_rows}行のOCCTO発電所運転実績データを保存しました")
            return imported_rows
        except Exception as e:
            logger.error(f"OCCTO発電所運転実績データのダウンロード中にエラーが発生: {e}")
            print(f"[ERROR] OCCTO発電所運転実績データのダウンロード中にエラーが発生: {e}")
            return 0
    
    def interactive_menu(self):
        """インタラクティブメニューを表示"""
        # メニュークラスは内部で独自のPowerMarketPortalを生成するため、
        # 依存性を外すためにシンプルなMenuの呼び出しに変更
        menu = Menu()
        menu.run()

def display_tso_choices():
    """TSO選択肢を表示して選択用のマッピングを返します"""
    tso_ids = {
        "hokkaido": {"name": "Hokkaido Electric Power Network", "area_code": "1"},
        "tohoku": {"name": "Tohoku Electric Power Network", "area_code": "2"},
        "tepco": {"name": "TEPCO Power Grid", "area_code": "3"},
        "chubu": {"name": "Chubu Electric Power Grid", "area_code": "4"},
        "hokuriku": {"name": "Hokuriku Electric Power Company", "area_code": "5"},
        "kansai": {"name": "Kansai Electric Power", "area_code": "6"},
        "chugoku": {"name": "Chugoku Electric Power", "area_code": "7"},
        "shikoku": {"name": "Shikoku Electric Power Company", "area_code": "8"},
        "kyushu": {"name": "Kyushu Electric Power", "area_code": "9"}
    }
    
    print("\nTSO Area Selection:")
    print("-" * 60)
    print(f"{'No.':<4} {'Area Code':<10} {'TSO Name':<30}")
    print("-" * 60)
    
    tso_choice_map = {}
    
    # 番号付きでTSOリストを表示
    for i, (tso_id, info) in enumerate(sorted(tso_ids.items(), key=lambda x: x[1]['area_code']), 1):
        print(f"{i:<4} {info['area_code']:<10} {info['name']:<30}")
        tso_choice_map[str(i)] = tso_id
    
    print("-" * 60)
    
    return tso_choice_map

def get_tso_selection(tso_choice_map):
    """ユーザーからTSO選択を取得します"""
    while True:
        choice = input("Select area (enter number): ").strip()
        
        if choice in tso_choice_map:
            return [tso_choice_map[choice]]  # 選択されたTSO
        else:
            print(f"Invalid selection. Please enter a number between 1-{len(tso_choice_map)}")

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
        default=(date.today() - timedelta(days=30)),
    )
    tso_data_parser.add_argument(
        "--end-date",
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
        help="終了日（YYYY-MM-DD形式）",
        default=(date.today() - timedelta(days=1)),
    )
    tso_data_parser.add_argument(
        "--tso-ids",
        type=str,
        nargs="+",
        required=False,
        help="処理対象のTSO ID（例: tepco hokkaido）- 省略した場合はインタラクティブに選択",
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
            # TSO IDが指定されていない場合はインタラクティブに選択
            tso_ids = args.tso_ids
            if not tso_ids:
                tso_choice_map = display_tso_choices()
                tso_ids = get_tso_selection(tso_choice_map)
                
            print(f"Downloading TSO data from {args.start_date} to {args.end_date} for areas: {', '.join(tso_ids)}...")
            rows = portal.download_tso_data(
                start_date=args.start_date,
                end_date=args.end_date,
                tso_ids=tso_ids
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