#!/usr/bin/env python3
"""
統合された日本の電力会社（TSO）データダウンローダー (リファクタリング版)

URL取得、ダウンロード、パース処理を外部モジュールに分離。
このスクリプトは全体のオーケストレーションとDB保存を担当。
"""

import os
import sys
import logging
import pandas as pd
from datetime import date, datetime
from typing import List, Tuple, Optional
import random
import time
import ssl

# SSL設定 (古いSSLプロトコル対応、downloader.pyにも同様の記述があるが念のため)
try:
    old_https_context = ssl._create_default_https_context
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

# プロジェクトのルートディレクトリをパスに追加 (既存のまま)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- 依存モジュールのインポート ---
from db.duckdb_connection import DuckDBConnection
# 分割したモジュールをインポート
from .tso_url_templates import get_tso_url, VALID_TSO_IDS # URL取得関数とTSO IDリスト
from .downloader import TSODataDownloader # ダウンロードクラス
from .parser import TSODataParser       # パーサークラス

# ロギング設定 (既存のまま)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UnifiedTSODownloader:
    """
    統合された日本の電力会社（TSO）データダウンローダー (オーケストレーター)
    ダウンロード、パース、DB保存の処理フローを管理する。
    """

    def __init__(
        self,
        tso_id: str = None,
        tso_ids: List[str] = None,
        db_connection: Optional[DuckDBConnection] = None,
        url_type: str = 'demand',
        table_name: Optional[str] = None # この引数はDB保存時に使われる
    ):
        """
        ダウンローダーを初期化
        Args:
            tso_id: 単一のTSO ID
            tso_ids: 複数のTSO IDリスト
            db_connection: DB接続オブジェクト (Parserと共有する場合あり)
            url_type: データ種類 ('demand' or 'supply')
            table_name: 保存先テーブル名 (オプション)
        """
        # DB接続の初期化 (なければ作成)
        self.db_connection = db_connection or DuckDBConnection()
        self.url_type = url_type
        self.table_name_prefix = table_name # 特定のテーブル名を指定する場合

        # 利用可能なTSO IDリストを外部モジュールから取得
        self.VALID_TSO_IDS = VALID_TSO_IDS

        # 処理対象のTSO IDを設定
        if tso_id:
            self.tso_ids = [tso_id]
        elif tso_ids:
            self.tso_ids = tso_ids
        else:
            # 指定がない場合はすべて
            self.tso_ids = self.VALID_TSO_IDS

        # TSO IDの検証
        invalid_ids = [tid for tid in self.tso_ids if tid not in self.VALID_TSO_IDS]
        if invalid_ids:
            raise ValueError(f"無効なTSO ID: {invalid_ids}。有効なID: {self.VALID_TSO_IDS}")

        # ダウンローダーとパーサーのインスタンスを作成
        # ParserにはDB接続を渡す (中部電力の重複チェック用)
        self.downloader = TSODataDownloader()
        self.parser = TSODataParser(db_connection=self.db_connection)

        logger.info(f"UnifiedTSODownloader初期化完了: TSOs=[{', '.join(self.tso_ids)}], URL Type={url_type}")

    def download_files(
        self,
        start_date: date,
        end_date: date,
        sleep_min: int = 1,
        sleep_max: int = 5,
        save_to_db: bool = True # DB保存を制御するフラグを追加
    ) -> List[Tuple[date, str, pd.DataFrame]]:
        """
        指定期間のすべてのデータファイルをダウンロード、パースし、(オプションで)DBに保存する。

        Args:
            start_date: 開始日
            end_date: 終了日
            sleep_min: リクエスト間の最小待機時間（秒）
            sleep_max: リクエスト間の最大待機時間（秒）
            save_to_db: Trueの場合、取得したデータをDBに保存する

        Returns:
            List[Tuple[date, str, DataFrame]]: 日付、TSO ID、処理済みデータフレームのタプルのリスト
                                              DB保存が有効な場合、保存後のDataFrameが返る。
                                              保存が無効な場合、パース後のDataFrameが返る。
        """
        processed_results = [] # パース/保存後の結果を格納

        if not self.tso_ids:
            logger.error("処理対象のTSO IDが指定されていません")
            return processed_results

        # 処理対象の月のリストを生成 (旧処理を踏襲)
        target_months = []
        current_date = start_date.replace(day=1)
        while current_date <= end_date:
            target_months.append(current_date)
            next_month_start = (
                date(current_date.year + 1, 1, 1)
                if current_date.month == 12
                else date(current_date.year, current_date.month + 1, 1)
            )
            current_date = next_month_start

        logger.info(f"処理対象期間: {start_date} - {end_date}")
        logger.info(f"処理対象月 (開始日基準): {[d.strftime('%Y-%m') for d in target_months]}")

        # 各TSO IDと月の組み合わせを処理
        for tso_id in self.tso_ids:
            # --- 中部電力の年間ZIP特別処理 ---
            # 月次ループの前に年間ZIPを試みる
            if tso_id == 'chubu':
                logger.info(f"中部電力 年間ZIP処理試行 (期間: {start_date} - {end_date})")
                try:
                    # 代表として期間内の適当な日付でURLを取得 (年は重要)
                    representative_date_for_year = start_date
                    base_zip_url = get_tso_url(tso_id, self.url_type, representative_date_for_year)

                    # fetch_data が年の探索を行う
                    # 中部電力の場合、fetch_dataはバイナリ(bytes)を返す想定
                    zip_content = self.downloader.fetch_data(tso_id, base_zip_url, representative_date_for_year)

                    if zip_content and isinstance(zip_content, bytes):
                        logger.info(f"中部電力 ZIPダウンロード成功 (サイズ: {len(zip_content)} bytes)")
                        # パーサーにZIPコンテンツを渡す
                        # _process_zip_file は重複チェック済みのDFを返す
                        # target_date はZIPファイル全体に関連するため、ここでは start_date を代表として渡す
                        combined_df = self.parser.parse_data(zip_content, tso_id, start_date, url=base_zip_url)

                        if not combined_df.empty:
                            logger.info(f"中部電力 ZIPパース成功: {len(combined_df)}行")
                            # ★ フィルタリングせず、combined_df (ZIP全体のパース結果) を使う
                            df_to_save = combined_df 

                            # DB保存処理 (オプション)
                            if save_to_db:
                                try:
                                    # ★ df_to_save を渡す
                                    self._save_to_database(df_to_save, start_date, tso_id)
                                    logger.info(f"中部電力 ZIPデータ ({len(df_to_save)}行) をDBに保存しました。")
                                    # processed_results には、代表日と全データフレームを追加
                                    processed_results.append((start_date, tso_id, df_to_save)) 
                                except Exception as db_err:
                                    logger.error(f"中部電力 ZIPデータのDB保存エラー: {db_err}")
                                    # DB保存失敗してもパース結果は返す
                                    processed_results.append((start_date, tso_id, df_to_save))
                            else:
                                 # DB保存しない場合はパース結果を結果に追加
                                 processed_results.append((start_date, tso_id, df_to_save))
                            # 中部電力はZIP処理が成功したら月次ループはスキップ
                            continue # 次のTSOへ
                        else:
                            logger.warning(f"中部電力 ZIPパース後、データが空でした。")
                    else:
                         logger.warning(f"中部電力 ZIPダウンロード失敗または空コンテンツ")
                except Exception as e:
                    logger.error(f"中部電力 年間ZIP処理中にエラーが発生しました: {e}。月次処理を試みます。")
                    # エラーが発生しても、後続の月次処理は試行する

            # --- 月次データ処理ループ ---
            for target_date in target_months:
                try:
                    logger.info(f"処理中: TSO={tso_id}, Month={target_date.strftime('%Y-%m')}, Type={self.url_type}")

                    # 待機処理 (初回以外)
                    if processed_results:
                        sleep_time = random.uniform(sleep_min, sleep_max)
                        logger.info(f"待機中: {sleep_time:.1f}秒...")
                        time.sleep(sleep_time)

                    # 1. URL取得
                    try:
                        base_url = get_tso_url(tso_id, self.url_type, target_date)
                    except ValueError as url_err:
                         logger.error(f"URL取得失敗: {url_err}")
                         continue # 次の月へ

                    # 2. データダウンロード
                    downloaded_content = None # 初期化
                    try:
                         # fetch_data は URL の特殊処理 (東北など) も行う
                         downloaded_content = self.downloader.fetch_data(tso_id, base_url, target_date)
                    except ValueError as dl_err:
                         logger.warning(f"ダウンロード失敗: {dl_err}")
                         continue # 次の月へ
                    except Exception as dl_fatal_err:
                         logger.error(f"予期せぬダウンロードエラー: {dl_fatal_err}", exc_info=True)
                         continue # 次の月へ

                    if not downloaded_content:
                        logger.warning(f"ダウンロードコンテンツが空: TSO={tso_id}, Month={target_date.strftime('%Y-%m')}")
                        continue # 次の月へ

                    # 3. データパース
                    parsed_df = pd.DataFrame() # 初期化
                    try:
                         # target_date は月を表すが、パース関数にはそのまま渡す
                         parsed_df = self.parser.parse_data(downloaded_content, tso_id, target_date, url=base_url)
                    except Exception as parse_err:
                         logger.error(f"パースエラー: {parse_err}", exc_info=True)
                         continue # 次の月へ

                    if parsed_df.empty:
                        logger.warning(f"パース結果が空: TSO={tso_id}, Month={target_date.strftime('%Y-%m')}")
                        continue # 次の月へ

                    logger.info(f"パース成功: {len(parsed_df)}行取得 (TSO={tso_id}, Month={target_date.strftime('%Y-%m')})")

                    # 4. DB保存 (オプション)
                    if save_to_db:
                        try:
                            # _save_to_databaseは月ごとのデータを保存
                            self._save_to_database(parsed_df, target_date, tso_id)
                            logger.info(f"DB保存完了: Table='{table_name}', Attempted={len(parsed_df)} rows")
                            processed_results.append((target_date, tso_id, parsed_df))
                        except Exception as db_err:
                             logger.error(f"DB保存エラー: {db_err}", exc_info=True)
                             # DB保存失敗してもパース結果は返す
                             processed_results.append((target_date, tso_id, parsed_df))
                    else:
                         # DB保存しない場合はパース結果をそのまま追加
                         processed_results.append((target_date, tso_id, parsed_df))

                except Exception as month_err:
                    # 月ごとのループで予期せぬエラーが発生した場合
                    logger.error(f"月次処理ループ(TSO={tso_id}, Month={target_date.strftime('%Y-%m')})でエラー: {month_err}", exc_info=True)
                    # 次の月/TSOの処理は続ける
                    continue

        logger.info(f"全処理完了。合計 {len(processed_results)} 件の結果を取得しました。")
        return processed_results

    def _save_to_database(self, df: pd.DataFrame, target_date: date, tso_id: str) -> None:
        """
        処理済みのDataFrameをデータベースに保存。
        unified_downloader.py からロジックを維持。
        テーブル名を動的に決定する。
        Args:
            df: 保存するDataFrame
            target_date: データの対象日 (主にログとメタデータ用)
            tso_id: TSO ID
        Raises:
            ValueError: DB接続がない場合
            Exception: DB保存時のエラー
        """
        if not self.db_connection:
            logger.error("データベース接続が利用できません。保存をスキップします。")
            raise ValueError("データベース接続が利用できません")

        if df.empty:
            logger.warning(f"保存対象のDataFrameが空です。TSO={tso_id}, Date={target_date}")
            return

        try:
            # TSO IDからエリアコードを取得（テーブル名生成用）
            area_code_map = {
                "hokkaido": "1", "tohoku": "2", "tepco": "3", "chubu": "4",
                "hokuriku": "5", "kansai": "6", "chugoku": "7", "shikoku": "8", "kyushu": "9",
                "okinawa": "10" # 沖縄のエリアコードを仮に10とする
            }
            area_code = area_code_map.get(tso_id)

            # テーブル名を決定
            if self.table_name_prefix:
                 # ユーザー指定のプレフィックスがある場合
                 table_name = f"{self.table_name_prefix}_{tso_id}" if area_code else self.table_name_prefix
            elif area_code:
                 # 標準的なテーブル名 (tso_area_X_data)
                 table_name = f"tso_area_{area_code}_data"
            else:
                 # フォールバック (tso_id_urltype)
                 table_name = f"{tso_id}_{self.url_type}"

            logger.info(f"DB保存開始: Table='{table_name}', Rows={len(df)}, TSO={tso_id}, Date={target_date}")

            # DataFrameをDBに保存 (DuckDBConnectionのメソッドを使用)
            self.db_connection.save_dataframe(df, table_name, if_exists='append')
            logger.info(f"DB保存完了: Table='{table_name}', Attempted={len(df)} rows") # 試行行数をログに出力

        except Exception as e:
            logger.error(f"データベース保存エラー: Table={table_name}, TSO={tso_id}, Error={e}", exc_info=True)
            raise # エラーを再発生させて呼び出し元に通知

    # _save_to_database_with_duplicate_check は save_dataframe 内で吸収されたか、
    # または Parser 側の _process_zip_file で重複チェックが既に行われているため削除。
    # 必要であれば save_dataframe に重複チェック機能を追加する。

# --- CLI実行部分 --- (既存のまま)
if __name__ == "__main__":
    import sys
    import os

    # プロジェクトのルートをインポートパスに追加
    # このファイルの場所基準でなく、起動スクリプトの場所基準が望ましい場合がある
    # ここでは既存のロジックを維持
    cli_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if cli_project_root not in sys.path:
        sys.path.insert(0, cli_project_root)

    # 対話式CLIスクリプトを実行
    try:
        # examplesディレクトリからの相対インポートを試みる
        from examples.interactive_tso_downloader import main as cli_main
        # UnifiedTSODownloader を使うように変更されているか確認が必要
        sys.exit(cli_main())
    except ImportError as e:
        print(f"エラー: 対話式CLIスクリプト (examples.interactive_tso_downloader) を読み込めませんでした - {e}")
        print("実行パスを確認するか、 `python examples/interactive_tso_downloader.py` を直接実行してください。")
        sys.exit(1)
    except Exception as e:
        logger.error(f"CLI実行中に予期せぬエラー: {e}", exc_info=True)
        sys.exit(1)