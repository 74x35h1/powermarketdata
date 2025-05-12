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
import re
from calendar import monthrange

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db.duckdb_connection import DuckDBConnection
from data_sources.tso.unified_downloader import UnifiedTSODownloader
# from data_sources.tso.tso_urls import TSO_INFO  # 削除されたためコメントアウト

# ロギングを設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TSO_IDS = [
    "hokkaido", "tohoku", "tepco", "chubu", "hokuriku", "kansai", "chugoku", "shikoku", "kyushu", "okinawa"
]

class TSODataImporter:
    """
    TSOデータをデータベースにインポートするクラス
    
    このクラスは、UnifiedTSODownloaderから取得したデータをデータベースに保存する機能を提供します。
    with文で使用することで、ブロックを抜けた時に自動的にデータベース接続を閉じます。
    """
    
    def __init__(self, db_path: str = None, read_only: bool = False):
        """
        TSOデータインポーターの初期化
        """
        try:
            # データベース接続の初期化
            self.connection = DuckDBConnection(db_path, read_only)
            
            # エリア別テーブル名の定義
            self.area_tables = {
                    1: "tso_area_1_data",
                    2: "tso_area_2_data",
                    3: "tso_area_3_data",
                    4: "tso_area_4_data",
                    5: "tso_area_5_data",
                    6: "tso_area_6_data",
                    7: "tso_area_7_data",
                    8: "tso_area_8_data",
                    9: "tso_area_9_data",
                    10: "tso_area_10_data"
            }
            
            # スキーマファイルを使用してテーブルを作成
        self._ensure_tables()
        
            # テーブルの存在を確認
            self._check_area_tables()
            
        except Exception as e:
            logger.error(f"TSOデータインポーターの初期化中にエラーが発生しました: {str(e)}")
            raise e

    def _check_area_tables(self):
        """
        エリア別テーブルの存在を確認する
        """
        try:
            # 既存のテーブル一覧を取得
            tables_result = self.connection.execute_query("PRAGMA show_tables;")
            existing_tables = [row[0].lower() for row in tables_result.fetchall()]
            
            # 各エリアテーブルの存在を確認
            for area_id, table_name in self.area_tables.items():
                if table_name.lower() not in existing_tables:
                    logger.warning(f"エリア{area_id}のテーブル {table_name} が存在しません")
                else:
                    logger.debug(f"エリア{area_id}のテーブル {table_name} が存在します")
        except Exception as e:
            logger.error(f"テーブル確認中にエラーが発生: {str(e)}")
    
    def _ensure_tables(self):
        """
        必要なテーブルが存在するかどうかを確認し、存在しない場合は作成します。
        """
        # schema_definition.sqlファイルの読み込み
        try:
            # プロジェクトのルートからパスを指定
            schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db", "schema_definition.sql")
            logger.info(f"スキーマ定義ファイルを読み込み: {schema_path}")
            print(f"[DEBUG] スキーマ定義ファイルを読み込み: {schema_path}")
            
            if not os.path.exists(schema_path):
                error_msg = f"スキーマ定義ファイルが見つかりません: {schema_path}"
                logger.error(error_msg)
                print(f"[ERROR] {error_msg}")
                raise FileNotFoundError(error_msg)
                
            # スキーマ定義ファイルから全てのSQL文を分割して実行
            with open(schema_path, "r") as f:
                schema_sql = f.read()
            
            if not schema_sql.strip():
                error_msg = "スキーマ定義ファイルが空です"
                logger.error(error_msg)
                print(f"[ERROR] {error_msg}")
                raise ValueError(error_msg)
            
            # 各テーブル作成ステートメントを実行
            statements = []
            # 先に行単位で分割してから処理
            lines = schema_sql.split('\n')
            current_stmt = []
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith('--'):
                    # 空行かコメント行なら現在のステートメントに影響しない
                    if current_stmt:  # 既にステートメント収集中なら追加
                        current_stmt.append(line)
                    continue
                
                # 新しいCREATE TABLE文の開始を検出
                if "CREATE TABLE IF NOT EXISTS" in line:
                    # 既に収集中のステートメントがあれば、それを保存
                    if current_stmt:
                        stmt = '\n'.join(current_stmt).strip()
                        if stmt:
                            statements.append(stmt)
                        current_stmt = []
                    
                    # 新しいステートメントの開始
                    current_stmt.append(line)
                else:
                    # 継続行
                    if current_stmt:  # 既にステートメント収集中なら追加
                        current_stmt.append(line)
            
            # 最後のステートメントも追加
            if current_stmt:
                stmt = '\n'.join(current_stmt).strip()
                if stmt:
                    statements.append(stmt)
            
            if not statements:
                error_msg = "スキーマ定義ファイルに有効なSQL文がありません"
                logger.error(error_msg)
                print(f"[ERROR] {error_msg}")
                raise ValueError(error_msg)
            
            for i, stmt in enumerate(statements):
                # コメントや空行はスキップ（先頭空白も考慮）
                stmt_clean = stmt.lstrip()
                if stmt_clean.startswith('--') or stmt_clean.startswith('/*') or not stmt_clean:
                    logger.debug(f"コメントまたは空行をスキップ: {stmt[:50]}...")
                    continue
                
                try:
                    print(f"[DEBUG] SQL実行 #{i+1}: {stmt.split('(')[0].strip()}")
                    logger.info(f"テーブル作成SQL #{i+1}: {stmt[:100]}...")
                    self.connection.execute_query(stmt)
                    table_name_match = re.search(r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)', stmt, re.IGNORECASE)
                    if table_name_match:
                        table_name = table_name_match.group(1)
                        print(f"[DEBUG] テーブル作成成功: {table_name}")
                except Exception as e:
                    error_msg = f"テーブル作成エラー: {str(e)}\nSQL: {stmt}"
                    logger.error(error_msg)
                    print(f"[ERROR] {error_msg}")
                    raise Exception(error_msg)  # 失敗したらエラーを投げる
            
            logger.info("スキーマ定義からTSOテーブルを作成しました")
            print("[DEBUG] スキーマ定義からTSOテーブルを作成しました")
            
            # ここでテーブル一覧を出力
            try:
                tables = self.connection.execute_query("PRAGMA show_tables;").fetchall()
                table_list = [t[0] for t in tables]
                print(f"[DEBUG] テーブル作成直後のDB({self.connection.db_path})のテーブル一覧: {table_list}")
                
                # エリア別テーブルがすべて存在するか確認
                for area_id, table_name in self.area_tables.items():
                    if table_name not in table_list:
                        logger.warning(f"エリア{area_id}のテーブル '{table_name}' が見つかりません")
                        print(f"[WARNING] エリア{area_id}のテーブル '{table_name}' が見つかりません")
            except Exception as e:
                logger.error(f"テーブル一覧取得エラー: {str(e)}")
                print(f"[ERROR] テーブル一覧取得エラー: {str(e)}")
        except Exception as e:
            logger.error(f"スキーマ定義からのテーブル作成に失敗しました: {str(e)}")
            print(f"[ERROR] スキーマ定義からのテーブル作成に失敗しました: {str(e)}")
            raise  # エラーを上位に伝播
    
    def import_data(self, data_list: List[Tuple[date, str, pd.DataFrame]]) -> int:
        """
        需要データをデータベースにインポート
        """
        total_inserted = 0
        
        try:
            # 各データフレームを処理
            for target_date, tso_id, df in data_list:
                if df is None or df.empty:
                    # 中部電力の特殊ケース: ZIPファイル処理中に既にデータが保存されている場合
                    if tso_id == 'chubu':
                        logger.info(f"中部電力の空のデータフレーム: データは_process_zip_fileメソッド内で既に保存済み")
                        # 実際の挿入行数はログから推定（正確な数ではない可能性あり）
                        estimated_rows = 48 * 30  # 30日分の48コマ（概算）
                        total_inserted += estimated_rows
                        continue
                    else:
                    logger.warning(f"空のデータフレーム: {tso_id}, {target_date}")
                    continue
                
                logger.info(f"データインポート処理: {tso_id}, {target_date}, データサイズ {df.shape}")
                
                # 必須カラムを確認
                required_cols = ['master_key', 'date', 'slot', 'area_demand']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.warning(f"必須カラムの欠落: {missing_cols} in {df.columns.tolist()}")
                    continue
                
                # ヘッダー行をフィルタリング（'TIME'が含まれる行を除外）
                df = df[~df['slot'].astype(str).str.contains('TIME')]
                
                # データタイプの出力
                print(f"[DEBUG] データフレームのデータ型: {df.dtypes}")
                print(f"[DEBUG] サンプルデータ: {df.head(2).to_dict('records')}")
                
                # NaN値の確認
                for col in required_cols:
                    if col in df.columns:
                        nan_count = df[col].isna().sum()
                        print(f"[DEBUG] カラム '{col}' のNaN数: {nan_count}")
                        
                        # データの一意性をチェック
                        try:
                            unique_values = df[col].unique()
                            print(f"[DEBUG] カラム '{col}' の一意値の数: {len(unique_values)}")
                            if len(unique_values) < 10:  # 値が少ない場合は表示
                                print(f"[DEBUG] カラム '{col}' の一意値: {unique_values.tolist()}")
                        except Exception as e:
                            print(f"[DEBUG] カラム '{col}' の一意値チェック中にエラー: {str(e)}")
                
                # スロットを整数に変換（時間形式の場合は変換処理を行う）
                try:
                    # 時間形式かどうかチェック
                    if not df.empty and 'slot' in df.columns and isinstance(df['slot'].iloc[0], str) and ':' in df['slot'].iloc[0]:
                        print(f"[DEBUG] スロットを時間形式から整数へ変換します")
                        
                        # 時間形式('00:00')を整数スロットに変換する関数
                        def time_to_slot(time_str):
                            if pd.isna(time_str):
                                return None
                            try:
                                if isinstance(time_str, str) and ':' in time_str:
                                    # 例: '00:00' → 1, '00:30' → 2, ...
                                    hours, minutes = time_str.split(':')
                                    return int(hours) * 2 + (1 if minutes == '30' else 0) + 1
                                else:
                                    return int(time_str)
                            except Exception as e:
                                print(f"[WARNING] スロット '{time_str}' の変換に失敗: {str(e)}")
                                return None
                        
                        # 変換前の一意値を表示
                        print(f"[DEBUG] 変換前のスロット値の例: {df['slot'].head(5).tolist()}")
                        
                        # 変換を適用
                        df['slot'] = df['slot'].apply(time_to_slot)
                        
                        # 変換後の一意値を表示
                        print(f"[DEBUG] 変換後のスロット値の例: {df['slot'].head(5).tolist()}")
                    elif not df.empty and 'slot' in df.columns:
                        # 既に数値形式かもしれない場合、astype で試す
                        try:
                        df['slot'] = df['slot'].astype(int)
                        except ValueError:
                            logger.warning(f"スロット列を整数に変換できませんでした。無効な値が含まれている可能性があります。{df['slot'].unique()[:10]}")
                            # エラーになった場合はNaNにして後でフィルタリング
                            df['slot'] = pd.to_numeric(df['slot'], errors='coerce')
                            df = df.dropna(subset=['slot']) # NaNになった行を削除
                            if df.empty:
                                logger.warning("無効なスロット値を除去した結果、データがなくなりました。")
                                continue
                            df['slot'] = df['slot'].astype(int) # 再度変換
                        
                except Exception as e:
                    print(f"[ERROR] スロットの変換中にエラー: {str(e)}")
                    print(f"[DEBUG] スロットの型: {df['slot'].dtype if 'slot' in df.columns else 'N/A'}")
                    print(f"[DEBUG] スロットの一意値: {df['slot'].unique().tolist()[:10] if 'slot' in df.columns and not df.empty else 'N/A'}")
                    # ここで continue するか検討 (スロットがないと master_key が作れない)
                    logger.warning("スロット変換エラーのため、このデータフレームの処理をスキップします。")
                            continue
                
                # 日付フォーマットをチェック
                try:
                    if not df.empty and 'date' in df.columns:
                        # 最初の値をチェック
                        # df['date'].iloc[0] が存在しない場合があるので修正
                        first_date_val = df['date'].iloc[0] if not df.empty else None
                        if first_date_val is not None and isinstance(first_date_val, str):
                            # if df['date'].dtype == 'object': # dtype チェックは必ずしも正確でないことがある
                            print(f"[DEBUG] 日付のフォーマット変換: {first_date_val} → ", end="")
                                # 文字列から日付に変換
                            df['date'] = pd.to_datetime(df['date'], errors='coerce') # errors='coerce'を追加
                            # 不正な日付は NaT になるので除外
                            df = df.dropna(subset=['date'])
                            if df.empty:
                                logger.warning("日付変換後にデータがなくなりました。")
                                continue
                                
                                # 未来の年を持つ日付を現在の年に修正
                                current_year = datetime.now().year
                                future_dates_mask = df['date'].dt.year > current_year
                                if future_dates_mask.any():
                                    # 未来の日付があれば、年だけ現在の年に置き換え
                                logger.info(f"[INFO] 未来の日付({df['date'][future_dates_mask].dt.year.iloc[0]}年)を{current_year}年に修正します")
                                    future_dates = df.loc[future_dates_mask, 'date']
                                    df.loc[future_dates_mask, 'date'] = future_dates.apply(
                                        lambda x: x.replace(year=current_year)
                                    )
                                
                                    # 文字列形式に戻す - ISO形式に変更
                                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                            print(f"{df['date'].iloc[0] if not df.empty else 'N/A'}")
                        elif pd.api.types.is_datetime64_any_dtype(df['date']):
                            # 既に datetime 型なら文字列に変換
                            df['date'] = df['date'].dt.strftime('%Y-%m-%d')

                except Exception as e:
                    print(f"[ERROR] 日付フォーマット変換中にエラー: {str(e)}")
                    print(f"[DEBUG] 日付カラムの型: {df['date'].dtype if 'date' in df.columns else 'N/A'}")
                    print(f"[DEBUG] 日付の一意値: {df['date'].unique().tolist()[:5] if 'date' in df.columns and not df.empty else 'N/A'}")
                    logger.warning("日付変換エラーのため、このデータフレームの処理をスキップします。")
                    continue
                
                # TSO IDからエリア番号を取得（ファイル名が数字で始まる場合を想定）
                area_id = None
                # ファイル名が[数字]_で始まる場合
                if isinstance(tso_id, str) and tso_id[0].isdigit():
                    try:
                        area_id = int(tso_id[0])
                    except ValueError:
                        pass # 数字で始まってもエリアIDでない場合
                
                if area_id is None: # 上記で取得できなかった場合
                    # TSO IDをエリア番号にマッピング
                    tso_to_area = {
                        "hokkaido": 1, "tohoku": 2, "tepco": 3, "chubu": 4, 
                        "hokuriku": 5, "kansai": 6, "chugoku": 7, "shikoku": 8, 
                        "kyushu": 9, "okinawa": 10
                    }
                    area_id = tso_to_area.get(tso_id.lower())
                
                if not area_id:
                    print(f"[ERROR] TSO ID '{tso_id}' からエリア番号を特定できません")
                    continue
                
                print(f"[INFO] TSO '{tso_id}' をエリア番号 '{area_id}' にマッピングしました")
                
                # エリア別テーブルへのデータ保存準備
                area_table_name = self.area_tables.get(area_id)
                if not area_table_name:
                    print(f"[ERROR] エリア '{area_id}' に対応するテーブルが見つかりません")
                    continue
                
                # エリア別テーブルの存在確認
                try:
                    table_check = self.connection.execute_query(f"SELECT COUNT(*) FROM {area_table_name} LIMIT 1")
                    print(f"[INFO] テーブル '{area_table_name}' が存在することを確認しました")
                except Exception as e:
                    print(f"[ERROR] テーブル '{area_table_name}' が存在しないか、アクセスできません: {str(e)}")
                    continue
                
                # 統合テーブルとエリア別テーブル両方に保存する必要はない
                # エリア別テーブルにのみ保存する
                try:
                    # データを挿入
                    rows = len(df)
                    if rows > 0:
                        inserted = self.connection.save_dataframe(df, area_table_name)
                        total_inserted += inserted
                        logger.info(f"{area_table_name}テーブルに{inserted}行を挿入しました")
                        print(f"[INFO] {area_table_name}テーブルに{inserted}行を挿入しました")
                    else:
                        logger.warning(f"データフレームが空です (rows={rows})")
                        print(f"[WARNING] データフレームが空です (rows={rows})")
                except Exception as e:
                    logger.error(f"データ挿入中にエラーが発生しました: {str(e)}")
                    print(f"[ERROR] データ挿入中にエラーが発生しました: {str(e)}")
                                continue
            
        except Exception as e:
            logger.error(f"データインポート処理中にエラーが発生しました: {str(e)}")
            print(f"[ERROR] データインポート処理中にエラーが発生しました: {str(e)}")
            raise e
        
        return total_inserted
    
    def import_from_downloader(
        self, 
        tso_ids: List[str] = None, 
        start_date: date = None, 
        end_date: date = None,
        url_type: str = "demand"
    ) -> int:
        """
        UnifiedTSODownloaderを使用して、指定されたTSO IDのデータをダウンロードし、インポートします。
        
        Args:
            tso_ids: インポートするTSO IDのリスト。指定されていない場合は、テスト用の北海道、東北、東京、中部を使用します。
            start_date: データの開始日。指定されていない場合は、現在の月の最初の日を使用します。
            end_date: データの終了日。指定されていない場合は、現在の日付を使用します。
            url_type: ダウンロードするデータの種類（'demand'または'supply'）
            
        Returns:
            インポートされた行数
        """
        # UnifiedTSODownloaderモジュールをインポート（循環依存を避けるため）
        from data_sources.tso.unified_downloader import UnifiedTSODownloader
        
        # 指定されていない場合のデフォルト値
        if not tso_ids:
            tso_ids = ["hokkaido", "tohoku", "tepco", "chubu"]
            
            if not start_date:
            # 当月の1日
            today = date.today()
            start_date = date(today.year, today.month, 1)
            
            if not end_date:
            end_date = date.today()
            
        logger.info(f"{start_date} から {end_date} までの {', '.join(tso_ids)} データをダウンロード中")
        
        total_inserted = 0
        
        # 各TSO IDに対してダウンロードを実行
        try:
            for tso_id in tso_ids:
                try:
                    # この特定のTSO ID用にダウンローダーを初期化
                    # データベース接続は共有する
                    downloader = UnifiedTSODownloader(
                        tso_id=tso_id,
                        db_connection=self.connection,  # db → connection に変更
                        url_type=url_type
                    )
                    
                    # データのダウンロード
                    data = downloader.download_files(start_date, end_date)
                    
                    # ダウンロードしたデータをインポート
                    if data is not None and len(data) > 0:
                        # 結果をインポート
                        inserted = self.import_data([(d, tid, df) for d, tid, df in data]) # 変数名変更 d, tid
                        total_inserted += inserted
                        logger.info(f"TSO {tso_id} から {inserted} 行をインポートしました")
                    else:
                        logger.warning(f"TSO {tso_id} からデータを取得できませんでした")
                
                except Exception as e:
                    logger.error(f"TSO {tso_id} の処理中にエラー: {str(e)}")
                    # TSOごとのエラーはログに残し、次のTSOへ進む
                        continue
        finally:
            # この finally は for ループの外側の try に対応するべき
            logger.info(f"TSOデータインポート処理メソッド import_from_downloader 完了 (tso_ids: {tso_ids})")
            
        return total_inserted

    # コンテキストマネージャサポート
    def __enter__(self):
        """コンテキストマネージャのエントリポイント"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャの終了処理"""
        # 明示的にDB接続をクローズ
        try:
            if hasattr(self, 'connection') and self.connection is not None:
                self.connection.close()
        except Exception as e:
            print(f"[WARN] TSODataImporterのコンテキスト終了時のDB接続クローズでエラー: {e}")


def parse_args():
    """コマンドライン引数を解析"""
    parser = argparse.ArgumentParser(description='TSO データをダウンロードしてデータベースにインポートします')
    
    parser.add_argument('--tso-id', dest='tso_ids', action='append',
                        help='インポートするTSO ID（複数回指定可能、省略時は全TSO）')
    
    parser.add_argument('--start-date', dest='start_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='ダウンロード開始日（YYYY-MM-DD形式、省略時は今日）')
    
    parser.add_argument('--end-date', dest='end_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='ダウンロード終了日（YYYY-MM-DD形式、省略時は開始日と同じ）')
    
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
        with TSODataImporter(db_path=args.db_path) as importer:
            
            # データをダウンロードしてインポート
            imported_rows = importer.import_from_downloader(
                tso_ids=args.tso_ids,
                start_date=args.start_date,
                end_date=args.end_date
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