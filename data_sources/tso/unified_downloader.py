#!/usr/bin/env python3
"""
統合された日本の電力会社（TSO）データダウンローダー

このスクリプトは、各電力会社（TSO）のWebサイトからデータを取得し、
標準化されたフォーマットでデータベースに保存するための機能を提供します。
"""

import os
import sys
import re
import io
import zipfile
import logging
import requests
import pandas as pd
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union
import random
import time

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db.duckdb_connection import DuckDBConnection
# from data_sources.tso.tso_urls import TSO_INFO, get_tso_url, TSO_IDS

# ロギングを設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UnifiedTSODownloader:
    """
    統合された日本の電力会社（TSO）データダウンローダー
    
    このクラスは、異なる電力会社のデータをダウンロードし処理するための
    共通のインターフェースとユーティリティを提供します。
    特定の電力会社（TSO）に対応するには、インスタンス化時にtso_idを指定します。
    """
    
    def __init__(
        self, 
        tso_id: str = None,
        tso_ids: List[str] = None,
        db_connection: Optional[DuckDBConnection] = None,
        url_type: str = 'demand',
        table_name: Optional[str] = None
    ):
        """
        TSOダウンローダーを初期化
        
        Args:
            tso_id: 単一のTSO ID（例: 'tepco', 'hokuriku'）
            tso_ids: 複数のTSO IDのリスト（tso_idが指定されていない場合使用）
            db_connection: データベース接続オブジェクト
            url_type: ダウンロードするデータの種類（'demand'または'supply'）
            table_name: データを保存するデータベーステーブル名
        """
        self.db_connection = db_connection or DuckDBConnection()
        self.url_type = url_type
        
        # 有効なTSO IDの一覧を定義（TSO_INFOに依存せず内部で管理）
        self.VALID_TSO_IDS = [
            "hokkaido", "tohoku", "tepco", "chubu", "hokuriku", 
            "kansai", "chugoku", "shikoku", "kyushu", "okinawa"
        ]
        
        # 単一のTSO IDと複数のTSO IDsの両方の指定をサポート
        if tso_id:
            self.tso_ids = [tso_id]
            self.tso_id = tso_id
            self.table_name = table_name or f"{tso_id}_{url_type}"
        elif tso_ids:
            self.tso_ids = tso_ids
            self.tso_id = None
            self.table_name = table_name or f"tso_{url_type}"
        else:
            self.tso_ids = self.VALID_TSO_IDS
            self.tso_id = None
            self.table_name = table_name or f"tso_{url_type}"
        
        # TSO IDの検証
        invalid_ids = [tid for tid in self.tso_ids if tid not in self.VALID_TSO_IDS]
        if invalid_ids:
            raise ValueError(f"無効なTSO ID: {invalid_ids}。有効なID: {self.VALID_TSO_IDS}")
            
        logger.info(f"TSO [{', '.join(self.tso_ids)}] のダウンローダーを初期化しました")
    
    def get_url(self, tso_id: str = None, target_date: date = None) -> str:
        """
        指定されたTSO IDのデータをダウンロードするURLを取得
        
        Args:
            tso_id: TSO ID。省略すると、インスタンス化時に指定したTSO IDを使用
            target_date: 対象日付。URLのプレースホルダーを置換するために使用
            
        Returns:
            URL文字列
            
        Raises:
            ValueError: URLが取得できない場合やTSO IDが指定されていない場合
        """
        # TSO IDの取得と検証
        tso_id = tso_id or self.tso_id
        
        if not tso_id:
            logger.error("TSO IDが指定されていません")
            raise ValueError("TSO IDが指定されていません")
        
        try:
            # tso_urls.pyに依存せず、直接URLを生成
            year_month = f"{target_date.year}{target_date.month:02d}"
            
            # 各TSOごとのURL形式を定義
            tso_url_templates = {
                "hokkaido": {
                    "demand": f"https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{year_month}_01.csv",
                    "supply": f"https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{year_month}_01.csv"
                },
                "tohoku": {
                    "demand": f"https://www.tohoku-epco.co.jp/NW/toririkumidata/juyo-download/juyo_tohoku_{year_month}.csv",
                    "supply": f"https://www.tohoku-epco.co.jp/NW/toririkumidata/juyo-download/juyo_tohoku_{year_month}.csv"
                },
                "tepco": {
                    "demand": f"https://www.tepco.co.jp/forecast/html/area_data/2025_area_data_{year_month}.csv",
                    "supply": f"https://www.tepco.co.jp/forecast/html/area_data/2025_area_data_{year_month}.csv"
                },
                "chubu": {
                    "demand": f"https://powergrid.chuden.co.jp/goannai/publication/supplydemand/archive/2025.zip",
                    "supply": f"https://powergrid.chuden.co.jp/goannai/publication/supplydemand/archive/2025.zip"
                },
                "hokuriku": {
                    "demand": f"https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{year_month}.csv",
                    "supply": f"https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{year_month}.csv"
                },
                "kansai": {
                    "demand": f"https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{year_month}.csv",
                    "supply": f"https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{year_month}.csv"
                },
                "chugoku": {
                    "demand": f"https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{year_month}.csv",
                    "supply": f"https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{year_month}.csv"
                },
                "shikoku": {
                    "demand": f"https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{year_month}_jukyu.csv",
                    "supply": f"https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{year_month}_jukyu.csv"
                },
                "kyushu": {
                    "demand": f"https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{year_month}.csv",
                    "supply": f"https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{year_month}.csv"
                },
                "okinawa": {
                    "demand": f"https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{year_month}.csv",
                    "supply": f"https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{year_month}.csv"
                }
            }
            
            # 該当するTSO IDのURLを取得
            if tso_id not in tso_url_templates:
                raise ValueError(f"無効なTSO ID: {tso_id}")
            
            if self.url_type not in tso_url_templates[tso_id]:
                raise ValueError(f"TSO {tso_id} に対してURL種別 {self.url_type} はサポートされていません")
            
            url = tso_url_templates[tso_id][self.url_type]
            logger.info(f"TSO {tso_id}, タイプ {self.url_type} のURL: {url}")
            return url
        except Exception as e:
            logger.error(f"URL取得中にエラーが発生しました: {str(e)}")
            raise ValueError(f"URLの取得に失敗しました: {str(e)}")
    
    def download_csv(self, target_date: date, tso_id: str = None, **kwargs) -> str:
        """
        指定された日付のCSVデータをダウンロード
        
        Args:
            target_date: ダウンロード対象の日付
            tso_id: TSO ID。省略すると、インスタンス化時に指定したTSO IDを使用
            **kwargs: ダウンロードの追加パラメータ
            
        Returns:
            CSV内容の文字列
            
        Raises:
            ValueError: ダウンロードに失敗した場合
        """
        tso_id = tso_id or self.tso_id
        
        if not tso_id:
            raise ValueError("単一のTSO IDが指定されていません")
            
        url = self.get_url(tso_id, target_date)
        
        # 日付パラメータをフォーマット（クエリパラメータとして使用する場合）
        params = {
            'year': target_date.year,
            'month': target_date.month,
            'day': target_date.day
        }
        
        # kwargsからの追加パラメータを追加
        params.update(kwargs.get('params', {}))
        
        try:
            logger.info(f"{url} から {target_date} のデータをダウンロード中")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # 中部電力のURL特殊処理（2025年ではなく現在利用可能な年を試す）
            if tso_id == 'chubu' and '2025.zip' in url:
                # 現在年以降は未来のデータなので存在しない可能性が高い
                # 現在の年または1年前のデータでテスト
                current_year = datetime.now().year
                alt_years = [current_year, current_year - 1]
                
                for alt_year in alt_years:
                    alt_url = url.replace('2025.zip', f"{alt_year}.zip")
                    logger.info(f"中部電力の代替URL: {alt_url} を試行")
                    
                    alt_response = requests.get(alt_url, headers=headers)
                    if alt_response.status_code == 200:
                        url = alt_url
                        break
                else:
                    logger.warning(f"中部電力の利用可能なデータが見つかりません")
            
            # 関西電力のURL特殊処理（2025年ではなく現在利用可能な年を試す）
            if tso_id == 'kepco' and '202501_jisseki.zip' in url:
                # 現在の年または1年前のデータでテスト
                current_year = datetime.now().year
                current_month = datetime.now().month
                
                test_dates = [
                    (current_year, current_month - 1),  # 先月
                    (current_year, current_month - 2),  # 先々月
                    (current_year - 1, 12)              # 去年の12月
                ]
                
                # 月が0以下になる場合の調整
                test_dates = [(y if m > 0 else y - 1, m if m > 0 else 12 + m) for y, m in test_dates]
                
                for test_year, test_month in test_dates:
                    alt_url = url.replace('202501_jisseki.zip', f"{test_year}{test_month:02d}_jisseki.zip")
                    logger.info(f"関西電力の代替URL: {alt_url} を試行")
                    
                    alt_response = requests.get(alt_url, headers=headers)
                    if alt_response.status_code == 200:
                        url = alt_url
                        # 日付を調整して正しい月のデータを処理
                        target_date = date(test_year, test_month, 1)
                        break
                else:
                    logger.warning(f"関西電力の利用可能なデータが見つかりません")
            
            # 中部電力や関西電力の場合、クエリパラメータは不要（URL自体に年が含まれるため）
            if (tso_id == 'chubu' or tso_id == 'kepco') and '.zip' in url:
                response = requests.get(url, headers=headers)
            else:
                response = requests.get(url, params=params, headers=headers)
                
            response.raise_for_status()
            
            # URLが.zipで終わる場合、ZIPファイルとして処理
            if url.lower().endswith('.zip'):
                return self._process_zip_file(response.content, target_date, tso_id)
            
            # レスポンスが空またはCSVとして有効でないか確認
            if not response.text.strip():
                raise ValueError(f"日付 {target_date} に対して空のレスポンスを受け取りました")
                
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"{url} からのダウンロードエラー: {str(e)}")
            raise ValueError(f"データのダウンロードに失敗しました: {str(e)}")
    
    def _process_zip_file(self, zip_content: bytes, target_date: date, tso_id: str) -> str:
        """
        ダウンロードしたZIPファイルを処理し、CSVコンテンツを抽出
        
        Args:
            zip_content: ZIPファイルのバイナリコンテンツ
            target_date: 対象日付
            tso_id: TSO ID
            
        Returns:
            抽出されたCSVコンテンツ
            
        Raises:
            ValueError: ZIPファイルの処理に失敗した場合
        """
        try:
            # メモリ上でZIPファイルを開く
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                # ZIPファイル内のファイル一覧を表示
                file_list = zip_file.namelist()
                logger.debug(f"ZIPファイル内のファイル: {file_list}")
                
                # CSVファイルを探す
                csv_files = [f for f in file_list if f.lower().endswith('.csv')]
                
                if not csv_files:
                    raise ValueError(f"ZIPファイル内にCSVファイルがありません: {file_list}")
                
                # 特別なケース：中部電力の月別ファイル検索
                if tso_id == 'chubu':
                    # 月別のパターン（例: eria_jukyu_202404.csv）を探す
                    year_month = f"{target_date.year}{target_date.month:02d}"
                    # 複数のパターンを試す（ファイル名の多様性に対応）
                    patterns = [
                        rf".*{year_month}.*\.csv",  # 完全な年月パターン
                        rf".*_{target_date.year}.*_{target_date.month:02d}.*\.csv",  # 年と月が分離
                        rf".*_{target_date.month:02d}月.*\.csv"  # 月のみ（日本語）
                    ]
                    
                    # すべてのパターンを試す
                    matched_files = []
                    for pattern in patterns:
                        pattern_re = re.compile(pattern, re.IGNORECASE)
                        matched = [f for f in csv_files if pattern_re.match(f)]
                        if matched:
                            matched_files.extend(matched)
                    
                    if matched_files:
                        logger.info(f"中部電力ZIPから一致したファイル: {matched_files}")
                        target_file = matched_files[0]
                    else:
                        # 月別ファイルが見つからない場合、すべてのファイルリストを表示
                        logger.warning(f"中部電力ZIPから一致するファイルがありません。利用可能なファイル: {csv_files}")
                        # 最初のCSVファイルを使用
                        target_file = csv_files[0]
                else:
                    # 通常のTSOのケース
                    year_month = f"{target_date.year}{target_date.month:02d}"
                    date_pattern = re.compile(rf".*{year_month}.*\.csv", re.IGNORECASE)
                    matched_files = [f for f in csv_files if date_pattern.match(f)]
                    
                    # 日付パターンに一致するファイルがある場合はそれを使用、なければ最初のCSVファイル
                    target_file = matched_files[0] if matched_files else csv_files[0]
                
                # 選択されたファイルをログに記録
                logger.info(f"ZIPから使用するファイル: {target_file}")
                
                # ファイルの内容を読み込み
                with zip_file.open(target_file) as file:
                    content = file.read()
                    # エンコーディングを自動検出して処理
                    try:
                        return content.decode('shift-jis', errors='replace')
                    except UnicodeDecodeError:
                        # 他のエンコーディングも試す
                        encodings = ['utf-8', 'cp932', 'euc-jp']
                        for encoding in encodings:
                            try:
                                return content.decode(encoding, errors='replace')
                            except UnicodeDecodeError:
                                continue
                        # すべて失敗した場合は強制的に変換
                        return content.decode('shift-jis', errors='ignore')
                    
        except (zipfile.BadZipFile, IndexError, UnicodeDecodeError) as e:
            logger.error(f"ZIPファイルの処理中にエラーが発生しました: {str(e)}")
            raise ValueError(f"ZIPファイルの処理に失敗しました: {str(e)}")
    
    def process_csv(self, csv_content: str, target_date: date, tso_id: str = None, **kwargs) -> pd.DataFrame:
        """
        ダウンロードしたCSV内容をDataFrameに処理
        
        Args:
            csv_content: CSV内容の文字列
            target_date: データをダウンロードした日付
            tso_id: TSO ID。省略すると、インスタンス化時に指定したTSO IDを使用
            **kwargs: 処理のための追加パラメータ
            
        Returns:
            処理済みのDataFrame
        """
        tso_id = tso_id or self.tso_id
        
        if not tso_id:
            raise ValueError("単一のTSO IDが指定されていません")
        
        try:
            # ヘッダー行をスキップしてCSVを読み込み、カラム名は使用せずインデックスでアクセスする
            try:
                df = pd.read_csv(
                    io.StringIO(csv_content),
                    encoding='shift-jis',
                    skiprows=kwargs.get('skiprows', 1),  # ヘッダー行をスキップ
                    header=None  # カラム名は使用せず
                )
            except UnicodeDecodeError:
                # Shift-JISで失敗した場合は他のエンコーディングを試す
                encodings = ['utf-8', 'cp932', 'euc-jp']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(
                            io.StringIO(csv_content),
                            encoding=encoding,
                            skiprows=kwargs.get('skiprows', 1),
                            header=None
                        )
                        break
                    except (UnicodeDecodeError, pd.errors.ParserError):
                        continue
                        
                if df is None:
                    raise ValueError("どのエンコーディングでもCSVデータを解析できませんでした")
            
            # url_typeに基づいて処理
            if self.url_type == 'demand':
                return self._process_demand_data(df, target_date, tso_id)
            elif self.url_type == 'supply':
                return self._process_supply_data(df, target_date, tso_id)
            else:
                raise ValueError(f"サポートされていないURLタイプ: {self.url_type}")
                
        except Exception as e:
            logger.error(f"CSVデータの処理エラー: {str(e)}")
            raise
    
    # 固定カラム順序の定義（TSOエリアデータの標準順序）
    # CSVファイルの順序: DATE,TIME,エリア需要,原子力,火力(LNG),火力(石炭),火力(石油),火力(その他),水力,地熱,
    # バイオマス,太陽光発電実績,太陽光出力制御量,風力発電実績,風力出力制御量,揚水,蓄電池,連系線,その他,合計
    DEMAND_COLUMNS_ORDER = [
        "master_key",   # 追加: yyyymmdd_slot形式
        "date",         # 0: DATE
        "slot",         # 1: TIME
        "area_demand",  # 2: エリア需要
        "nuclear",      # 3: 原子力
        "LNG",          # 4: 火力(LNG)
        "coal",         # 5: 火力(石炭)
        "oil",          # 6: 火力(石油)
        "other_fire",   # 7: 火力(その他)
        "hydro",        # 8: 水力
        "geothermal",   # 9: 地熱
        "biomass",      # 10: バイオマス
        "solar_actual", # 11: 太陽光発電実績
        "solar_control",# 12: 太陽光出力制御量
        "wind_actual",  # 13: 風力発電実績
        "wind_control", # 14: 風力出力制御量
        "pumped_storage",# 15: 揚水
        "battery",      # 16: 蓄電池
        "interconnection",# 17: 連系線
        "other",        # 18: その他
        "total"         # 19: 合計
    ]

    def _process_demand_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        需要データを標準形式に処理（固定カラム構造を前提）
        """
        logger.info(f"元のデータフレーム形状: {df.shape}")
        logger.info(f"TSO: {tso_id}、対象日付: {target_date}")
        
        # 必要な列数があるか確認
        expected_columns = len(self.DEMAND_COLUMNS_ORDER) - 1  # master_keyは後で追加するため-1
        if df.shape[1] < expected_columns:
            logger.error(f"カラム数が不足しています。期待:{expected_columns}, 実際:{df.shape[1]}")
            # 足りない分は空のDataFrameを返す
            return pd.DataFrame(columns=self.DEMAND_COLUMNS_ORDER)
        
        # 必要な列だけを抽出し、名前を変更
        df_result = df.iloc[:, :expected_columns].copy()  # 必要な列だけ抽出
        temp_columns = self.DEMAND_COLUMNS_ORDER[1:]  # master_keyを除いたカラム名
        df_result.columns = temp_columns  # 英語カラム名を割り当て
        
        # ヘッダー行を検出して除外
        # 最初の行がヘッダー行かどうかを確認するために、日付列の値を確認
        if len(df_result) > 0 and isinstance(df_result['date'].iloc[0], str):
            first_row_date = df_result['date'].iloc[0].strip().lower()
            if first_row_date == 'date' or first_row_date in ['日付', 'date', 'date', '日付']:
                logger.info("ヘッダー行を検出しました。この行を除外します。")
                df_result = df_result.iloc[1:].copy()  # ヘッダー行を除外
        
        # 日付列を適切に変換 - 時間部分なしの日付のみ
        try:
            # 文字列として取得し、日付部分のみを抽出
            df_result['date'] = pd.to_datetime(df_result['date'], errors='coerce')
            # 無効な日付の行を除外
            df_result = df_result.dropna(subset=['date'])
            # 日付のみを文字列で保持 (YYYY-MM-DD形式)
            df_result['date'] = df_result['date'].dt.strftime('%Y-%m-%d')
            logger.info(f"日付変換後のサンプル: {df_result['date'].head(3).tolist()}")
        except Exception as e:
            logger.error(f"日付変換エラー: {str(e)}")
            logger.debug(f"変換前の値（最初の5つ）: {df_result['date'].head(5).tolist()}")
        
        # 時間スロットの形式を統一
        try:
            # 時間帯形式の正規化のための正規表現
            time_pattern = r'(\d{1,2})[:\s時](\d{1,2})'
            
            def normalize_time_format(time_str):
                if pd.isna(time_str):
                    return None
                if time_str == 'TIME' or time_str.lower() == 'time':
                    return None
                
                time_str = str(time_str).strip()
                match = re.search(time_pattern, time_str)
                if match:
                    hour, minute = match.groups()
                    return f"{int(hour):02d}:{int(minute):02d}"
                return time_str
            
            df_result['slot'] = df_result['slot'].apply(normalize_time_format)
            # 無効なスロットの行を除外
            df_result = df_result.dropna(subset=['slot'])
            logger.info(f"スロット変換後のサンプル: {df_result['slot'].head(3).tolist()}")
        except Exception as e:
            logger.error(f"時間スロット変換エラー: {str(e)}")
            logger.debug(f"変換前の値（最初の5つ）: {df_result['slot'].head(5).tolist()}")
        
        # マスターキーの生成（yyyymmdd_slot形式）
        try:
            def create_master_key(row):
                try:
                    # 日付からyyyymmdd形式を作成
                    date_str = row['date']
                    if pd.isna(date_str) or date_str is None:
                        return None
                    
                    try:
                        date_obj = pd.to_datetime(date_str)
                        date_part = date_obj.strftime('%Y%m%d')
                    except:
                        # 既に'YYYYMMDD'形式の場合
                        date_part = date_str.replace('-', '')
                    
                    # スロットからHH:MM形式を取得し、:を削除
                    slot = row['slot']
                    if pd.isna(slot) or slot is None:
                        return None
                    
                    slot_part = slot.replace(':', '')
                    
                    # マスターキーを結合
                    return f"{date_part}_{slot_part}"
                except Exception as e:
                    logger.error(f"マスターキー生成エラー: {str(e)}, row={row}")
                    return None
            
            df_result['master_key'] = df_result.apply(create_master_key, axis=1)
            # 無効なマスターキーの行を除外
            df_result = df_result.dropna(subset=['master_key'])
            
            # マスターキーカラムを先頭に移動
            cols = df_result.columns.tolist()
            cols.remove('master_key')
            cols = ['master_key'] + cols
            df_result = df_result[cols]
            
            # 重複マスターキーを削除
            before = len(df_result)
            df_result = df_result.drop_duplicates(subset=['master_key'], keep='first')
            after = len(df_result)
            if before != after:
                logger.warning(f"重複マスターキーのため {before - after} 行を削除しました")
            
            logger.info(f"マスターキー生成サンプル: {df_result['master_key'].head(3).tolist()}")
        except Exception as e:
            logger.error(f"マスターキー列の生成エラー: {str(e)}")
        
        # 数値型カラムを数値に変換
        numeric_columns = [col for col in df_result.columns if col not in ['date', 'slot', 'master_key', 'tso_id']]
        for col in numeric_columns:
            try:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
            except Exception as e:
                logger.error(f"{col}列の数値変換エラー: {str(e)}")
        
        # tso_id追加
        df_result['tso_id'] = tso_id
        
        # データが正しく読み込めたことの確認ログ
        logger.info(f"処理後のデータ形状: {df_result.shape}")
        if not df_result.empty:
            logger.info(f"データサンプル: {df_result.head(1).to_dict('records')}")
            # 重複チェック
            if df_result.duplicated(subset=['master_key']).any():
                dup_count = df_result.duplicated(subset=['master_key']).sum()
                logger.warning(f"マスターキーの重複があります: {dup_count}件")
                # 重複している最初の数件を表示
                dup_keys = df_result[df_result.duplicated(subset=['master_key'], keep=False)]['master_key'].head(5).tolist()
                logger.warning(f"重複マスターキーの例: {dup_keys}")
                # 重複を削除
                df_result = df_result.drop_duplicates(subset=['master_key'], keep='first')
                logger.info(f"重複削除後の形状: {df_result.shape}")
        else:
            logger.info("データが空です")
        
        return df_result

    def _process_supply_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        供給データを標準形式に処理（需要データと同じカラム構造を前提）
        """
        # 需要データと同じ処理を適用
        return self._process_demand_data(df, target_date, tso_id)
    
    def download_files(
        self,
        start_date: date,
        end_date: date,
        sleep_min: int = 1,
        sleep_max: int = 5,
        **kwargs
    ) -> List[Tuple[date, str, pd.DataFrame]]:
        """
        日付範囲に対応するファイルをダウンロード（月単位で1回だけダウンロードし、1回の結果として返す）
        
        Args:
            start_date: ダウンロード開始日
            end_date: ダウンロード終了日（含む）
            sleep_min: ダウンロード間の最小スリープ時間（秒）
            sleep_max: ダウンロード間の最大スリープ時間（秒）
            **kwargs: ダウンロードと処理のための追加パラメータ
        
        Returns:
            (対象月の代表日付, tso_id, dataframe) のタプルのリスト
        """
        results = []
        try:
            for tso_id in self.tso_ids:
                # 月ごとに1回だけダウンロード（月をまたぐ場合のみ複数回）
                current_month = (start_date.year, start_date.month)
                end_month = (end_date.year, end_date.month)
                
                # 処理対象の月リストを生成
                months_to_process = []
                y, m = current_month
                while (y, m) <= end_month:
                    months_to_process.append((y, m))
                    m += 1
                    if m > 12:
                        y += 1
                        m = 1
                
                logger.info(f"処理対象月: {months_to_process}")
                
                for year, month in months_to_process:
                    try:
                        # 月の初日を基準日として使用
                        target_date = date(year, month, 1)
                        logger.info(f"{target_date.strftime('%Y-%m')} の {tso_id} {self.url_type} データをダウンロード中")
                        
                        # 月全体のデータをダウンロード
                        csv_content = self.download_csv(target_date, tso_id, **kwargs)
                        df = self.process_csv(csv_content, target_date, tso_id, **kwargs)
                        
                        # 処理に失敗した場合はスキップ
                        if df.empty:
                            logger.warning(f"{year}年{month}月の{tso_id}データが空です")
                            continue
                        
                        # 指定範囲の日付のみに絞り込む（元々の方法と同じ）
                        df['date'] = pd.to_datetime(df['date'])
                        mask = (df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))
                        filtered_df = df[mask].copy()
                        
                        if filtered_df.empty:
                            logger.warning(f"指定範囲 {start_date} ~ {end_date} のデータがありません")
                            continue
                        
                        # 日付を文字列に戻す
                        filtered_df['date'] = filtered_df['date'].dt.strftime('%Y-%m-%d')
                        
                        # 月全体のデータを一括で返す
                        results.append((target_date, tso_id, filtered_df))
                        
                        # データ量のログ出力
                        logger.info(f"{year}年{month}月の{tso_id}データ: {len(filtered_df)}行")
                        
                        # サーバー負荷軽減
                        time.sleep(random.uniform(sleep_min, sleep_max))
                        
                    except Exception as e:
                        logger.error(f"{year}年{month}月の{tso_id} {self.url_type}データのダウンロードエラー: {str(e)}")
        finally:
            # データベース接続がある場合は必ず閉じる
            if hasattr(self, 'db_connection') and self.db_connection is not None:
                try:
                    self.db_connection.close()
                    print(f"[INFO] ダウンロード後のデータベース接続を閉じました")
                except Exception as e:
                    print(f"[ERROR] データベース接続を閉じる際にエラーが発生: {str(e)}")
        
        return results
    
    def _save_to_database(self, df: pd.DataFrame, target_date: date, tso_id: str = None) -> None:
        """
        処理済みのDataFrameをデータベースに保存
        
        Args:
            df: 保存するDataFrame
            target_date: データをダウンロードした日付
            tso_id: TSO ID。省略すると、インスタンス化時に指定したTSO IDを使用
            
        Raises:
            ValueError: データベース接続が利用できない場合
        """
        if not self.db_connection:
            raise ValueError("データベース接続が利用できません")
        
        tso_id = tso_id or self.tso_id
            
        try:
            table_name = self.table_name
            if not tso_id and '_{tso_id}_' not in table_name:
                # 複数のTSOをダウンロードする場合は、table_nameにtso_idが含まれていることを確認
                table_name = f"tso_{self.url_type}"
                
            logger.info(f"{target_date} に対して {table_name} に {len(df)} 行を保存しています")
            
            # 存在しない場合はメタデータ列を追加
            if 'year' not in df.columns:
                df['year'] = target_date.year
            if 'month' not in df.columns:
                df['month'] = target_date.month
            if 'day' not in df.columns:
                df['day'] = target_date.day
            if 'tso' not in df.columns and 'tso_id' not in df.columns:
                df['tso'] = tso_id
                
            # DataFrameを保存
            self.db_connection.save_dataframe(df, table_name)
            logger.info(f"{table_name} にデータを正常に保存しました")
            
        except Exception as e:
            logger.error(f"データベースへのデータ保存エラー: {str(e)}")
            raise

if __name__ == "__main__":
    # ファイルが直接実行された場合、対話式CLIを起動
    import sys
    import os
    
    # プロジェクトのルートをインポートパスに追加
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    # 対話式CLIスクリプトを実行
    try:
        from examples.interactive_tso_downloader import main
        sys.exit(main())
    except ImportError as e:
        print(f"エラー: 対話式CLIスクリプトを読み込めませんでした - {e}")
        print("コマンドを実行して対話式CLIを起動してください: ./run_tso_cli.sh")
        sys.exit(1) 