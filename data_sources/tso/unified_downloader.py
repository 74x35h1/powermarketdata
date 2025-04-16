#!/usr/bin/env python3
"""
統合された TSO (送電系統運用者) データダウンローダー

このモジュールは、日本の電力会社（TSO）からデータをダウンロードし処理するための
統合されたクラスを提供します。各TSOごとに別々のクラスを作成する代わりに、
設定を変更することで任意のTSOに対応します。
"""

import logging
import requests
import pandas as pd
import io
import time
import random
import zipfile
import re
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import os
import sys
import json

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.db_connection import DuckDBConnection
from data_sources.tso.tso_urls import get_tso_url, TSO_INFO

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
            self.tso_ids = list(TSO_INFO.keys())
            self.tso_id = None
            self.table_name = table_name or f"tso_{url_type}"
        
        # TSO IDの検証
        invalid_ids = [tid for tid in self.tso_ids if tid not in TSO_INFO]
        if invalid_ids:
            raise ValueError(f"無効なTSO ID: {invalid_ids}。有効なID: {list(TSO_INFO.keys())}")
            
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
            ValueError: URLが取得できない場合
        """
        tso_id = tso_id or self.tso_id
        
        if not tso_id:
            raise ValueError("TSO IDが指定されていません")
            
        url = get_tso_url(tso_id, self.url_type)
        if not url:
            raise ValueError(f"TSO {tso_id}, タイプ {self.url_type} のURLを取得できませんでした")
        
        # 日付が指定されている場合、URLにあるプレースホルダーを置換
        if target_date:
            # {YYYY} -> 年、{MM} -> 月のフォーマットで置換
            url = url.replace('{YYYY}', str(target_date.year))
            url = url.replace('{MM}', f"{target_date.month:02d}")  # 2桁の月
        
        return url
    
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
            # 異なるエンコーディングをテスト
            encodings = ['shift-jis', 'utf-8', 'cp932', 'euc-jp']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(
                        io.StringIO(csv_content),
                        encoding=encoding,
                        skiprows=kwargs.get('skiprows', 1)  # ヘッダー行をスキップ
                    )
                    break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            
            if df is None:
                raise ValueError("どのエンコーディングでもCSVデータを解析できませんでした")
            
            # 列名から空白を削除
            df.columns = df.columns.str.strip()
            
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
    
    def _process_demand_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        需要データを標準形式に処理
        
        Args:
            df: 生のDataFrame
            target_date: データの日付
            tso_id: TSO ID
            
        Returns:
            処理済みのDataFrame
        """
        # デバッグ出力を追加
        logger.debug(f"元のデータ型: {df.dtypes}")
        logger.debug(f"データの先頭: {df.head().to_dict()}")
        
        # 列名を標準形式にリネーム
        column_map = {
            '時間帯': 'time_slot',
            '時刻': 'time_slot',
            '需要実績': 'demand_actual',
            '需要実績(万kW)': 'demand_actual',
            'エリア需要': 'demand_actual',
            '予測値': 'demand_forecast',
            '予測値(万kW)': 'demand_forecast'
        }
        
        # 関西電力の場合は特別な列名処理
        if tso_id == 'kepco':
            # 関西電力のCSVファイルは列名が異なる可能性があるため追加
            additional_cols = {
                '実績値': 'demand_actual',
                '需要（万kW）': 'demand_actual',
                '時間': 'time_slot',
                'TIME': 'time_slot',
                '日付': 'date_jp'
            }
            column_map.update(additional_cols)
        
        # 存在する列のみリネームを適用
        rename_dict = {k: v for k, v in column_map.items() if k in df.columns}
        if rename_dict:
            df = df.rename(columns=rename_dict)
        else:
            logger.warning(f"期待される列名がデータに見つかりませんでした: {df.columns.tolist()}")
        
        # 日付列の追加
        df['date'] = target_date
        
        # TSO ID列の追加
        df['tso_id'] = tso_id
        
        # time_slotを標準形式に変換（存在する場合）
        if 'time_slot' in df.columns:
            try:
                # 時間帯から時間を抽出（形式は様々）
                # NaNを含む可能性があるため、astype(str)の前にNaNを置換
                df['time_slot'] = df['time_slot'].fillna('')
                # 時間のパターンを抽出
                hour_values = df['time_slot'].astype(str).str.extract(r'(\d+)').fillna(-1)
                # 変換エラーが発生しないよう、明示的にintに変換する前にNaNをチェック
                df['hour'] = pd.to_numeric(hour_values[0], errors='coerce').fillna(-1).astype(int)
                # -1は無効な時間なので、取り除く
                df = df[df['hour'] >= 0]
            except Exception as e:
                logger.error(f"時間帯の変換エラー: {str(e)}")
                # エラーが発生した場合はhour列を追加せずに続行
                if 'hour' not in df.columns:
                    df['hour'] = -1
        
        # 値を万kWからkWに変換（10000を掛ける）
        # NaNは数値に変換できないため、事前にエラー処理
        if 'demand_actual' in df.columns:
            # NaNを0.0に置換し、数値型に変換
            df['demand_actual'] = pd.to_numeric(df['demand_actual'], errors='coerce').fillna(0.0)
            df['demand_actual'] = df['demand_actual'].astype(float) * 10000
            
        if 'demand_forecast' in df.columns:
            # NaNを0.0に置換し、数値型に変換
            df['demand_forecast'] = pd.to_numeric(df['demand_forecast'], errors='coerce').fillna(0.0)
            df['demand_forecast'] = df['demand_forecast'].astype(float) * 10000
        
        return df
    
    def _process_supply_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        供給データを標準形式に処理
        
        Args:
            df: 生のDataFrame
            target_date: データの日付
            tso_id: TSO ID
            
        Returns:
            処理済みのDataFrame
        """
        # 列名を標準形式にリネーム
        column_map = {
            '時間帯': 'time_slot',
            '時刻': 'time_slot',
            '供給力': 'supply_capacity',
            '供給力(万kW)': 'supply_capacity',
            '原子力': 'nuclear',
            '火力': 'thermal',
            '水力': 'hydro',
            '太陽光': 'solar',
            '風力': 'wind',
            '揚水': 'pumped_storage',
            'バイオマス': 'biomass',
            '地熱': 'geothermal',
            'その他': 'other'
        }
        
        # 存在する列のみリネームを適用
        rename_dict = {k: v for k, v in column_map.items() if k in df.columns}
        if rename_dict:
            df = df.rename(columns=rename_dict)
        else:
            logger.warning(f"期待される列名がデータに見つかりませんでした: {df.columns.tolist()}")
        
        # 日付列の追加
        df['date'] = target_date
        
        # TSO ID列の追加
        df['tso_id'] = tso_id
        
        # time_slotを標準形式に変換（存在する場合）
        if 'time_slot' in df.columns:
            # 時間帯から時間を抽出（形式は様々）
            df['hour'] = df['time_slot'].astype(str).str.extract(r'(\d+)').astype(int)
        
        # 値を万kWからkWに変換（10000を掛ける）
        numerical_columns = [
            'supply_capacity', 'nuclear', 'thermal', 'hydro', 'solar', 
            'wind', 'pumped_storage', 'biomass', 'geothermal', 'other'
        ]
        
        for col in [c for c in numerical_columns if c in df.columns]:
            df[col] = df[col].astype(float) * 10000
        
        return df
    
    def download_files(
        self,
        start_date: date,
        end_date: date,
        sleep_min: int = 1,
        sleep_max: int = 5,
        **kwargs
    ) -> List[Tuple[date, str, pd.DataFrame]]:
        """
        日付範囲に対応するファイルをダウンロード
        
        Args:
            start_date: ダウンロード開始日
            end_date: ダウンロード終了日（含む）
            sleep_min: ダウンロード間の最小スリープ時間（秒）
            sleep_max: ダウンロード間の最大スリープ時間（秒）
            **kwargs: ダウンロードと処理のための追加パラメータ
            
        Returns:
            (日付, tso_id, dataframe) のタプルのリスト
        """
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            for tso_id in self.tso_ids:
                try:
                    logger.info(f"{current_date} の {tso_id} {self.url_type} データをダウンロード中")
                    
                    # CSVコンテンツをダウンロード
                    csv_content = self.download_csv(current_date, tso_id, **kwargs)
                    
                    # CSVをDataFrameに処理
                    df = self.process_csv(csv_content, current_date, tso_id, **kwargs)
                    
                    # データベースに保存（接続が利用可能な場合）
                    if self.db_connection:
                        self._save_to_database(df, current_date, tso_id)
                    
                    results.append((current_date, tso_id, df))
                    logger.info(f"{current_date} の {tso_id} {self.url_type} データを正常に処理しました")
                    
                    # サーバーに負荷をかけないようにスリープ
                    sleep_time = random.uniform(sleep_min, sleep_max)
                    logger.debug(f"{sleep_time:.2f} 秒間スリープします")
                    time.sleep(sleep_time)
                        
                except Exception as e:
                    logger.error(f"{current_date} の {tso_id} {self.url_type} データのダウンロードエラー: {str(e)}")
            
            current_date += timedelta(days=1)
            
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