#!/usr/bin/env python
"""
電力会社（TSO）データダウンロード例

このスクリプトは、UnifiedTSODownloaderクラスを使用して
日本の電力会社からデータをダウンロードする方法を示します。
"""

import logging
import argparse
from datetime import datetime, timedelta
import sys
import os

# 親ディレクトリをパスに追加してモジュールをインポートできるようにする
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources.tso.unified_downloader import UnifiedTSODownloader
from db.duckdb_connection import DuckDBConnection
# from data_sources.tso.tso_urls import get_tso_url, TSO_INFO

# ロギングを設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TSO_IDS = [
    "hokkaido", "tohoku", "tepco", "chubu", "hokuriku", "kansai", "chugoku", "shikoku", "kyushu", "okinawa"
]
TSO_INFO = {
    "hokkaido": {"name": "北海道電力", "area_code": 1},
    "tohoku": {"name": "東北電力", "area_code": 2},
    "tepco": {"name": "東京電力", "area_code": 3},
    "chubu": {"name": "中部電力", "area_code": 4},
    "hokuriku": {"name": "北陸電力", "area_code": 5},
    "kansai": {"name": "関西電力", "area_code": 6},
    "chugoku": {"name": "中国電力", "area_code": 7},
    "shikoku": {"name": "四国電力", "area_code": 8},
    "kyushu": {"name": "九州電力", "area_code": 9},
    "okinawa": {"name": "沖縄電力", "area_code": 10}
}

def parse_args():
    """コマンドライン引数を解析します。"""
    parser = argparse.ArgumentParser(description='日本の電力会社からデータをダウンロード')
    
    parser.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='ダウンロードを開始する日付 (YYYY-MM-DD)',
        default=(datetime.now() - timedelta(days=7)).date()
    )
    
    parser.add_argument(
        '--end-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='ダウンロードを終了する日付 (YYYY-MM-DD)',
        default=datetime.now().date()
    )
    
    parser.add_argument(
        '--url-type',
        choices=['demand', 'supply'],
        default='demand',
        help='ダウンロードするデータの種類（demand または supply）'
    )
    
    parser.add_argument(
        '--tso-id',
        choices=TSO_IDS,
        help='特定の電力会社のデータをダウンロードする場合は、そのTSO IDを指定します'
    )
    
    parser.add_argument(
        '--db-path',
        default='powermarket.duckdb',
        help='DuckDBデータベースファイルのパス'
    )
    
    return parser.parse_args()

def main():
    """電力会社（TSO）データをダウンロードするメイン関数。"""
    args = parse_args()
    
    if args.tso_id:
        logger.info(f"{args.tso_id} の {args.url_type} データをダウンロードします")
        logger.info(f"日付範囲: {args.start_date} から {args.end_date}")
    else:
        logger.info(f"すべての電力会社の {args.url_type} データをダウンロードします")
        logger.info(f"日付範囲: {args.start_date} から {args.end_date}")
    
    # データベース接続を作成
    db_connection = DuckDBConnection(args.db_path)
    
    # ダウンローダーを作成
    downloader = UnifiedTSODownloader(
        tso_id=args.tso_id,  # None の場合はすべての TSO がダウンロードされます
        db_connection=db_connection,
        url_type=args.url_type
    )
    
    try:
        # 指定された日付範囲のファイルをダウンロード
        results = downloader.download_files(
            start_date=args.start_date,
            end_date=args.end_date,
            sleep_min=2,
            sleep_max=5
        )
        
        # 結果のサマリーを表示
        tso_counts = {}
        for _, tso_id, _ in results:
            tso_counts[tso_id] = tso_counts.get(tso_id, 0) + 1
        
        logger.info(f"合計 {len(results)} 日分のデータをダウンロードしました")
        
        for tso_id, count in tso_counts.items():
            logger.info(f"{TSO_INFO[tso_id]['name']}: {count} 日分")
            
    except Exception as e:
        logger.error(f"データのダウンロード中にエラーが発生しました: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 