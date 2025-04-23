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
    level=logging.INFO,
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
    """
    
    def __init__(self):
        """
        TSOデータインポーターを初期化
        """
        self.db = DuckDBConnection()
        self.table_name = "tso_data"  # デフォルトテーブル名
        self._ensure_tables()
        
        # エリアコードとテーブル名のマッピング
        self.area_tables = {
            "1": "tso_area_1_data",
            "2": "tso_area_2_data",
            "3": "tso_area_3_data",
            "4": "tso_area_4_data",
            "5": "tso_area_5_data",
            "6": "tso_area_6_data",
            "7": "tso_area_7_data",
            "8": "tso_area_8_data",
            "9": "tso_area_9_data"
        }
    
    def _ensure_tables(self):
        """データベーステーブルが存在することを確認"""
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
                    self.db.execute_query(stmt)
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
                tables = self.db.execute_query("PRAGMA show_tables;").fetchall()
                table_list = [t[0] for t in tables]
                print(f"[DEBUG] テーブル作成直後のDB({self.db.db_path})のテーブル一覧: {table_list}")
                
                # tso_dataテーブルが存在するか確認
                if self.table_name not in table_list:
                    error_msg = f"必要なテーブル '{self.table_name}' が作成されませんでした"
                    logger.error(error_msg)
                    print(f"[ERROR] {error_msg}")
                    print(f"[ERROR] schema_definition.sqlを確認してください。テーブルのCREATE TABLE文が正しく記述されていることを確認してください。")
                    raise Exception(error_msg)
                
                # tso_dataテーブルのスキーマを確認
                try:
                    schema_info = self.db.execute_query(f"PRAGMA table_info({self.table_name});").fetchall()
                    print(f"[DEBUG] {self.table_name}テーブルのスキーマ:")
                    for col in schema_info:
                        print(f"[DEBUG]   {col[1]} ({col[2]})")
                except Exception as e:
                    print(f"[WARNING] テーブルスキーマの取得中にエラー: {str(e)}")
            except Exception as e:
                logger.error(f"テーブル一覧取得エラー: {str(e)}")
                print(f"[ERROR] テーブル一覧取得エラー: {str(e)}")
                raise
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
            # データベーストランザクション開始
            self.db.execute_query("BEGIN TRANSACTION")
            
            # 各データフレームを処理
            for target_date, tso_id, df in data_list:
                if df is None or df.empty:
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
                    if isinstance(df['slot'].iloc[0], str) and ':' in df['slot'].iloc[0]:
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
                    else:
                        # 既に数値形式の場合
                        df['slot'] = df['slot'].astype(int)
                        
                except Exception as e:
                    print(f"[ERROR] スロットの変換中にエラー: {str(e)}")
                    print(f"[DEBUG] スロットの型: {df['slot'].dtype}")
                    print(f"[DEBUG] スロットの一意値: {df['slot'].unique().tolist()[:10]}")
                    
                    # 数値変換できない値をフィルタリング
                    print(f"[INFO] 無効なスロット値をフィルタリングします")
                    df = df.dropna(subset=['slot'])
                    if not df.empty:
                        df = df[pd.to_numeric(df['slot'], errors='coerce').notna()]
                        if df.empty:
                            print(f"[WARNING] スロット変換後にデータがありません")
                            continue
                        df['slot'] = df['slot'].astype(int)
                
                # 日付フォーマットをチェック
                try:
                    if not df.empty and 'date' in df.columns:
                        # 最初の値をチェック
                        if df['date'].iloc[0] is not None and isinstance(df['date'].iloc[0], str):
                            if df['date'].dtype == 'object':  # 文字列の場合のみ変換
                                print(f"[DEBUG] 日付のフォーマット変換: {df['date'].iloc[0]} → ", end="")
                                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
                                print(f"{df['date'].iloc[0]}")
                except Exception as e:
                    print(f"[ERROR] 日付フォーマット変換中にエラー: {str(e)}")
                    print(f"[DEBUG] 日付カラムの型: {df['date'].dtype}")
                    print(f"[DEBUG] 日付の一意値: {df['date'].unique().tolist()[:5]}")
                
                # TSO IDからエリア番号を取得（ファイル名が数字で始まる場合を想定）
                area_code = None
                # ファイル名が[数字]_で始まる場合
                if isinstance(tso_id, str) and tso_id[0].isdigit():
                    area_code = tso_id[0]
                else:
                    # TSO IDをエリア番号にマッピング
                    tso_to_area = {
                        "hokkaido": "1", "tohoku": "2", "tepco": "3", "chubu": "4", 
                        "hokuriku": "5", "kansai": "6", "chugoku": "7", "shikoku": "8", 
                        "kyushu": "9", "okinawa": "0"
                    }
                    area_code = tso_to_area.get(tso_id.lower(), None)
                
                if not area_code:
                    print(f"[ERROR] TSO ID '{tso_id}' からエリア番号を特定できません")
                    continue
                
                print(f"[INFO] TSO '{tso_id}' をエリア番号 '{area_code}' にマッピングしました")
                
                # エリア別テーブルへのデータ保存準備
                area_table_name = self.area_tables.get(area_code)
                if not area_table_name:
                    print(f"[ERROR] エリア '{area_code}' に対応するテーブルが見つかりません")
                    continue
                
                # エリア別テーブルの存在確認
                try:
                    table_check = self.db.execute_query(f"SELECT COUNT(*) FROM {area_table_name} LIMIT 1")
                    print(f"[INFO] テーブル '{area_table_name}' が存在することを確認しました")
                except Exception as e:
                    print(f"[ERROR] テーブル '{area_table_name}' が存在しないか、アクセスできません: {str(e)}")
                    continue
                
                # 統合テーブルとエリア別テーブル両方に保存する
                # 1. 統合テーブル(tso_data)用の変換準備
                tso_table_columns = []
                try:
                    schema_info = self.db.execute_query(f"PRAGMA table_info({self.table_name});").fetchall()
                    tso_table_columns = [col[1] for col in schema_info]
                    print(f"[DEBUG] テーブル'{self.table_name}'の実際のカラム: {tso_table_columns[:10]}...")
                except Exception as e:
                    print(f"[ERROR] テーブルスキーマの取得に失敗: {str(e)}")
                
                # カラム名を変換（area_demand → X_area_demand）- 統合テーブル用
                renamed_columns_integrated = {}
                for col in df.columns:
                    if col in ['master_key', 'date', 'slot', 'tso_id']:  # 共通カラムはそのまま
                        renamed_columns_integrated[col] = col
                    else:
                        # X_カラム名 の形式に変換
                        area_prefixed_col = f"{area_code}_{col}"
                        if area_prefixed_col in tso_table_columns:
                            renamed_columns_integrated[col] = area_prefixed_col
                
                # 2. エリア別テーブル用のカラム確認
                area_table_columns = []
                try:
                    area_schema_info = self.db.execute_query(f"PRAGMA table_info({area_table_name});").fetchall()
                    area_table_columns = [col[1] for col in area_schema_info]
                    print(f"[DEBUG] テーブル'{area_table_name}'の実際のカラム: {area_table_columns[:10]}...")
                except Exception as e:
                    print(f"[ERROR] エリアテーブルスキーマの取得に失敗: {str(e)}")
                    
                # エリア別テーブルと統合テーブルの両方に保存
                # 1. まずエリア別テーブルに保存 (プレフィックスなしのカラム名)
                valid_area_columns = [col for col in df.columns if col in area_table_columns]
                if len(valid_area_columns) < 2:
                    print(f"[ERROR] エリアテーブル用の有効なカラムが不足しています: {valid_area_columns}")
                else:
                    df_area = df[valid_area_columns].copy()
                    # カラム名を明示的に指定してデータを挿入 (エリア別テーブル用)
                    area_column_list = df_area.columns.tolist()
                    area_placeholders = ", ".join(["?"] * len(area_column_list))
                    area_column_names = ", ".join(area_column_list)
                    
                    # エリア別テーブルにデータを挿入
                    area_query = f"INSERT INTO {area_table_name} ({area_column_names}) VALUES ({area_placeholders})"
                    
                    try:
                        # 最後の行を出力して確認
                        if not df_area.empty:
                            print(f"[DEBUG] エリア用の最後の挿入行: {df_area.iloc[-1].to_dict()}")
                        
                        area_row_count = 0
                        area_error_count = 0
                        for _, row in df_area.iterrows():
                            # NaN値チェック
                            row_values = row.tolist()
                            if 'master_key' in df_area.columns and pd.isna(row['master_key']):
                                continue
                                
                            try:
                                self.db.execute_query(area_query, row_values)
                                total_inserted += 1
                                area_row_count += 1
                            except Exception as e:
                                area_error_count += 1
                                if area_error_count <= 5:
                                    print(f"[ERROR] エリア別テーブルへのデータ挿入中にエラー: {str(e)}")
                                    print(f"[DEBUG] エラーが発生した行: {row.to_dict()}")
                                elif area_error_count == 6:
                                    print(f"[WARNING] 追加のエラーがあります。メッセージの表示を制限します...")
                        
                        if area_error_count > 0:
                            print(f"[INFO] エリアテーブル: {area_row_count}行挿入、{area_error_count}行エラー")
                        else:
                            print(f"[INFO] エリアテーブル '{area_table_name}' に {area_row_count}行挿入しました")
                        
                        logger.info(f"{target_date}, {tso_id} のデータを {area_table_name} に保存しました。件数: {area_row_count}")
                    except Exception as e:
                        logger.error(f"エリア別テーブルへのデータ保存中にエラー: {str(e)}")
                        print(f"[ERROR] エリア別テーブルへのデータ保存中にエラー: {str(e)}")
                
                # 2. 次に統合テーブルに保存 (プレフィックス付きのカラム名)
                if tso_table_columns:  # 統合テーブルのカラム情報が取得できた場合のみ
                    # データフレームのカラム名を変換（統合テーブル用）
                    df_renamed = df.rename(columns=renamed_columns_integrated)
                    
                    # テーブルに存在するカラムだけを抽出
                    valid_columns = [col for col in df_renamed.columns if col in tso_table_columns]
                    if len(valid_columns) < 2:  # master_keyだけでは意味がない
                        print(f"[ERROR] 統合テーブル用の有効なカラムが不足しています: {valid_columns}")
                    else:
                        df_final = df_renamed[valid_columns]
                        print(f"[INFO] 統合テーブル用のデータ（{df_final.shape[0]}行, {df_final.shape[1]}列）: {valid_columns[:10]}...")
                        
                        # カラム名を明示的に指定してデータを挿入
                        column_list = df_final.columns.tolist()
                        placeholders = ", ".join(["?"] * len(column_list))
                        column_names = ", ".join([f'"{col}"' if col.startswith(('1', '2', '3', '4', '5', '6', '7', '8', '9')) else col for col in column_list])
                        
                        # データを挿入
                        query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"
                        
                        try:
                            row_count = 0
                            error_count = 0
                            for _, row in df_final.iterrows():
                                # NaN値チェック
                                row_values = row.tolist()
                                if 'master_key' in df_final.columns and pd.isna(row['master_key']):
                                    continue
                                    
                                try:
                                    self.db.execute_query(query, row_values)
                                    row_count += 1
                                except Exception as e:
                                    error_count += 1
                                    if error_count <= 5:
                                        print(f"[ERROR] 統合テーブルへのデータ挿入中にエラー: {str(e)}")
                                        print(f"[DEBUG] エラーが発生した行: {row.to_dict()}")
                                    elif error_count == 6:
                                        print(f"[WARNING] 追加のエラーがあります。メッセージの表示を制限します...")
                            
                            if error_count > 0:
                                print(f"[INFO] 統合テーブル: {row_count}行挿入、{error_count}行エラー")
                            else:
                                print(f"[INFO] 統合テーブル '{self.table_name}' に {row_count}行挿入しました")
                        except Exception as e:
                            logger.error(f"統合テーブルへのデータ保存中にエラー: {str(e)}")
                            print(f"[ERROR] 統合テーブルへのデータ保存中にエラー: {str(e)}")
            
            # トランザクションをコミット
            self.db.execute_query("COMMIT")
            print(f"[INFO] トランザクションを正常にコミットしました。合計 {total_inserted} 行を挿入しました。")
            
        except Exception as e:
            # エラーが発生した場合はロールバック
            try:
                self.db.execute_query("ROLLBACK")
                print(f"[INFO] トランザクションをロールバックしました。")
            except Exception as rollback_error:
                logger.error(f"ロールバックに失敗: {str(rollback_error)}")
                print(f"[ERROR] ロールバックに失敗: {str(rollback_error)}")
            
            logger.error(f"データインポート中にエラーが発生: {str(e)}")
            print(f"[ERROR] データインポート中にエラーが発生: {str(e)}")
            raise
        finally:
            # 処理完了時に必ずDBとの接続を閉じる
            try:
                self.db.close()
                print(f"[INFO] データベース接続を閉じました。")
            except Exception as close_error:
                print(f"[ERROR] データベース接続を閉じる際にエラーが発生: {str(close_error)}")
        
        return total_inserted
    
    def import_from_downloader(
        self, 
        tso_ids: List[str] = None, 
        start_date: date = None, 
        end_date: date = None,
        url_type: str = "demand"
    ) -> int:
        """
        指定されたTSOとデータ期間のデータをダウンロードしてインポート
        Args:
            tso_ids: インポートするTSO IDのリスト（省略すると全TSO）
            start_date: ダウンロード開始日（省略すると今日）
            end_date: ダウンロード終了日（省略すると開始日）
            url_type: ダウンロードするデータの種類（'demand'または'supply'）
        Returns:
            インポートされた行数の合計
        """
        imported_rows = 0
        try:
            # 日付のデフォルト値を設定
            if start_date is None:
                start_date = date.today()
            if end_date is None:
                end_date = start_date
            
            # 全TSO IDsをデフォルトとして使用
            if tso_ids is None:
                tso_ids = TSO_IDS
                
            logger.info(f"{start_date} から {end_date} までの {', '.join(tso_ids)} データをダウンロード中")
            
            # ダウンローダーを初期化
            downloader = UnifiedTSODownloader(
                tso_ids=tso_ids,
                db_connection=None,  # DB接続はここでは使用しない
                url_type=url_type
            )
            
            # データをダウンロード（一括で）
            results = downloader.download_files(start_date, end_date)
            
            # ダウンロードしたデータをインポート
            imported_rows = self.import_data(results)
            
            logger.info(f"合計 {imported_rows} 行のデータをインポートしました")
            return imported_rows
        
        except Exception as e:
            logger.error(f"データのインポートに失敗しました: {str(e)}")
            print(f"[ERROR] データのインポートに失敗しました: {str(e)}")
            raise
        
        finally:
            # 処理完了時に必ずDBとの接続を閉じる
            try:
                if hasattr(self, 'db') and self.db is not None:
                    self.db.close()
                    print(f"[INFO] データベース接続を閉じました。")
            except Exception as close_error:
                print(f"[ERROR] データベース接続を閉じる際にエラーが発生: {str(close_error)}")
        
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
        importer = TSODataImporter()
        
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