import pandas as pd
import logging
import io
import zipfile
import re
from datetime import date, datetime
from typing import Union, Optional

# chardetは必要になった場合のみインポート
# import chardet

logger = logging.getLogger(__name__)

class TSODataParser:
    """
    ダウンロードされたTSOデータ（CSVまたはZIP）を解析し、
    標準化されたDataFrameに変換するクラス。
    """
    def __init__(self, db_connection=None):
        """
        パーサを初期化します。
        Args:
            db_connection: DuckDB接続オブジェクト（中部電力の重複チェック用）
        """
        self.db_connection = db_connection
        # 固定カラム順序の定義（unified_downloader.pyから移動）
        self.DEMAND_COLUMNS_ORDER = [
            "master_key", "date", "slot", "area_demand", "nuclear", "LNG", "coal", "oil",
            "other_fire", "hydro", "geothermal", "biomass", "solar_actual", "solar_control",
            "wind_actual", "wind_control", "pumped_storage", "battery", "interconnection",
            "other", "total"
        ]

    def parse_data(self, content: Union[bytes, str], tso_id: str, target_date: date, url: str = '') -> pd.DataFrame:
        """
        ダウンロードされたコンテンツを解析し、DataFrameを返す。
        unified_downloader.py の process_csv の役割を担う。

        Args:
            content: ダウンロードされたコンテンツ (bytes または str)
            tso_id: TSO ID
            target_date: 対象日付
            url: ダウンロード元のURL (ZIPファイル処理などで使用)

        Returns:
            pd.DataFrame: 処理されたデータフレーム (失敗時は空のDataFrame)
        """
        logger.info(f"Parsing data for TSO={tso_id}, Date={target_date}, URL={url}")
        if not content:
            logger.warning(f"空のコンテンツを受け取りました: TSO={tso_id}, Date={target_date}")
            return pd.DataFrame()

        # --- 中部電力ZIPファイルの特別処理 --- (unified_downloader.py から移植)
        # contentがbytesで、URLが.zipで終わるか、content自体がZIPファイルか判定
        is_zip = False
        if isinstance(content, bytes):
            if url and url.lower().endswith('.zip'):
                is_zip = True
            else:
                 try:
                     # content自体がZIPか判定
                     with zipfile.ZipFile(io.BytesIO(content), 'r') as zf:
                         is_zip = True
                 except zipfile.BadZipFile:
                     is_zip = False # ZIPファイルではない
                 except Exception as e:
                     logger.warning(f"ZIP判定中に予期せぬエラー: {e}")
                     is_zip = False

        if tso_id == 'chubu' and is_zip:
            logger.info(f"中部電力のZIPファイルを処理します: URL={url}")
            return self._process_zip_file(content, url) # ZIP処理メソッドへ

        # --- CSVファイルの処理 --- (ZIPでない場合)
        try:
            csv_text = ''
            if isinstance(content, bytes):
                encoding = self._detect_encoding(content)
                logger.info(f"検出されたエンコーディング: {encoding}")
                try:
                    csv_text = content.decode(encoding)
                except UnicodeDecodeError as e:
                    logger.error(f"エンコード {encoding} でのデコード失敗: {e}")
                    # フォールバックエンコーディング (例: shift-jis, cp932)
                    fallback_encodings = ['shift-jis', 'cp932']
                    for enc in fallback_encodings:
                        if enc != encoding: # 最初に試したエンコード以外
                            try:
                                csv_text = content.decode(enc)
                                logger.warning(f"フォールバックエンコード {enc} でデコード成功")
                                break
                            except UnicodeDecodeError:
                                continue
                    else: # すべてのフォールバックが失敗した場合
                         logger.error("すべてのエンコードでのデコードに失敗しました")
                         return pd.DataFrame()
            elif isinstance(content, str):
                csv_text = content
            else:
                 logger.error(f"予期しないコンテンツタイプ: {type(content)}")
                 return pd.DataFrame()

            if not csv_text.strip():
                logger.warning(f"デコード後のCSVテキストが空です: TSO={tso_id}, Date={target_date}")
                return pd.DataFrame()

            # CSVをDataFrameとして読み込む (ヘッダー自動推論)
            try:
                # まずヘッダーありで試す -> やめて、常に header=1 で読み込む
                # df = pd.read_csv(io.StringIO(csv_text))
                # # 最初の行がデータに見えない場合（例：文字列が多い）、ヘッダーなしで再試行
                # if len(df) > 0 and df.iloc[0].apply(lambda x: isinstance(x, str)).mean() > 0.8:
                #      logger.info("最初の行がヘッダーではなさそうなので header=None で再試行")
                #      df = pd.read_csv(io.StringIO(csv_text), header=None)
                
                # ★ 全てのTSOでヘッダーが2行目(index=1)にあると仮定して読み込む
                # ★ skipinitialspace=True も追加
                logger.info("CSV読み込み試行 (header=1, skipinitialspace=True)")
                df = pd.read_csv(io.StringIO(csv_text), header=1, skipinitialspace=True)

            except pd.errors.ParserError as e:
                 logger.warning(f"Pandas ParserError (header=1): {e}. ヘッダーなしで再試行します。")
                 try:
                     # ★ header=1が失敗した場合のみ、フォールバックとしてheader=Noneを試す
                     df = pd.read_csv(io.StringIO(csv_text), header=None, skipinitialspace=True)
                 except Exception as e2:
                     logger.error(f"CSVの読み込みに失敗しました (header=None フォールバック): {e2}")

            if df.empty:
                logger.warning(f"読み込んだDataFrameが空です: TSO={tso_id}, Date={target_date}")
                return pd.DataFrame()

            logger.info(f"CSV読み込み成功: {len(df)}行, {len(df.columns)}列")

            # --- TSOごとのデータ整形 --- 
            # 現状、中部電力以外は共通の整形処理を使う想定だが、
            # 将来的にTSOごとに異なる整形が必要な場合はここで分岐する。
            # if tso_id == 'tepco':
            #     processed_df = self._process_tepco_data(df, target_date)
            # elif ...
            # else:
            #     processed_df = self._process_generic_demand_data(df, target_date, tso_id)
            if tso_id == 'chubu':
                # 基本的にZIPで処理されるはずだが、CSVが来た場合の処理
                processed_df = self._set_column_names_for_chubu(df)
            else:
                # 他のTSOは共通の需要データ処理を試みる
                processed_df = self._process_demand_data(df, target_date, tso_id)

            if processed_df.empty:
                 logger.warning(f"データ整形後のDataFrameが空です: TSO={tso_id}")

            return processed_df

        except Exception as e:
            logger.error(f"CSV処理中に予期せぬエラー: {e}", exc_info=True)
            return pd.DataFrame()

    def _detect_encoding(self, binary_content: bytes) -> str:
        """
        バイナリデータのエンコーディングを検出する。
        unified_downloader.py から移植。
        Returns: 検出されたエンコーディング、失敗時は'utf-8' (より一般的)
        """
        # chardet が重いので、よく使われるものを先に試す
        common_encodings = ['utf-8', 'shift-jis', 'cp932']
        for enc in common_encodings:
            try:
                binary_content.decode(enc)
                logger.debug(f"エンコーディング検出成功（試行）: {enc}")
                return enc
            except UnicodeDecodeError:
                continue

        # 一般的なエンコーディングが失敗した場合のみchardetを使用
        try:
            import chardet
            result = chardet.detect(binary_content)
            encoding = result.get('encoding')
            confidence = result.get('confidence', 0)

            if encoding and confidence > 0.7:
                logger.info(f"chardetによるエンコーディング検出: {encoding} (信頼度: {confidence:.2f})")
                return encoding
            else:
                 logger.warning(f"chardet検出失敗 or 低信頼度: {result}. デフォルト 'utf-8' を使用")
                 return 'utf-8' # デフォルトを shift-jis から utf-8 に変更

        except ImportError:
            logger.warning("chardetライブラリが見つかりません。エンコーディング検出は基本的なもののみ行います。デフォルト 'utf-8' を使用")
            return 'utf-8'
        except Exception as e:
            logger.error(f"エンコーディング検出中にエラー: {e}")
            return 'utf-8' # エラー時もデフォルト

    def _process_zip_file(self, zip_content: bytes, url: str) -> pd.DataFrame:
        """
        ZIPファイルからすべてのCSVファイルを処理して単一のデータフレームに統合。
        unified_downloader.py から移植。
        ★ DBアクセスによる重複チェックは削除。パーサは変換のみ行う。
        Returns: 処理結果のデータフレーム (失敗時は空のDataFrame)
        """
        all_processed_dfs = []
        try:
            # URLから年を抽出（ログ用）
            target_year_match = re.search(r'eria_jukyu_(\d{4})\.zip', url)
            target_year = int(target_year_match.group(1)) if target_year_match else None
            logger.info(f"ZIPファイル処理開始: URL={url}, TargetYear={target_year}")

            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                logger.info(f"ZIP内のCSVファイル数: {len(csv_files)}")

                if not csv_files:
                    logger.warning(f"ZIPファイル内にCSVが見つかりません: {url}")
                    return pd.DataFrame()

                # --- 各CSVファイルを処理 ---
                for csv_file_name in csv_files:
                    logger.info(f"ZIP内のCSVを処理中: {csv_file_name}")
                    try:
                        with zip_ref.open(csv_file_name) as csv_file:
                            csv_content_bytes = csv_file.read()
                            if not csv_content_bytes:
                                logger.warning(f"空のCSVファイル: {csv_file_name}")
                                continue

                            # バイトデータをパース
                            encoding = self._detect_encoding(csv_content_bytes)
                            csv_text = csv_content_bytes.decode(encoding)

                            # ★ header=1 を指定して2行目をヘッダーとして読み込む
                            # ★ skipinitialspace=True を追加して区切り文字後のスペースを無視
                            df_raw = pd.read_csv(io.StringIO(csv_text), header=1, skipinitialspace=True)
                            if df_raw.empty:
                                logger.warning(f"CSV読み込み後データが空: {csv_file_name}")
                                continue

                            # 中部電力固有の整形処理を適用 (他のTSOもここを通る可能性を考慮)
                            # 整形処理の中でヘッダー行(読み込まれたDataFrameの0行目)の再確認と変換を行う
                            # TSO ID を渡す (master_key生成で使う)
                            processed_df = self._set_column_names_for_chubu(df_raw, 'chubu') # ZIPファイルはchubuと仮定

                            if processed_df.empty:
                                logger.warning(f"整形後データが空: {csv_file_name}")
                                continue

                            # 空でない場合のみリストに追加 (重複排除がなくなったため、整形後の結果をそのまま追加)
                            if not processed_df.empty:
                                 all_processed_dfs.append(processed_df)
                            else:
                                 logger.debug(f"処理後データが空（元々空か整形失敗）: {csv_file_name}")

                    except Exception as csv_error:
                        logger.error(f"ZIP内のCSV '{csv_file_name}' 処理エラー: {csv_error}", exc_info=True)

            # --- 全データフレームを結合 ---
            if not all_processed_dfs:
                logger.warning(f"ZIPファイルから有効なデータを抽出できませんでした: {url}")
                return pd.DataFrame()

            combined_df = pd.concat(all_processed_dfs, ignore_index=True)
            logger.info(f"ZIPファイル処理完了: {len(combined_df)}行結合 (URL: {url})")

            return combined_df

        except zipfile.BadZipFile:
            logger.error(f"不正なZIPファイル形式です: {url}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"ZIPファイル処理中に予期せぬエラー: {e}", exc_info=True)
            return pd.DataFrame()

    # ==================================================================
    # データ整形メソッド (旧 unified_downloader.py から移植)
    # ==================================================================

    def _process_demand_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        需要データを標準形式に処理（CSVサンプルに合わせて修正）
        - ヘッダーは1行目（ファイル先頭から2行目）と仮定 (pd.read_csv(header=1))
        - DATE (YYYY/MM/DD) -> date (YYYYMMDD str)
        - TIME (HH:MM) -> slot (int 1-48)
        - master_key -> YYYYMMDD_Slot
        """
        logger.info(f"需要データ整形開始: TSO={tso_id}, Date={target_date}, 元Shape={df.shape}")

        # header=1 で読み込んでいる前提なので、ヘッダー行削除は不要
        df_data = df.copy()
        if df_data.empty:
             logger.warning("入力データが空です。")
             return pd.DataFrame()

        # 列名を英語に変換するためのマッピング (小文字で比較)
        column_mapping = {
            'date': 'date', 'time': 'slot',
            'エリア需要': 'area_demand', '実績(万kw)': 'area_demand', # ()内の大文字小文字揺れ考慮
            '原子力': 'nuclear',
            '火力(lng)': 'LNG', '火力（ｌｎｇ）': 'LNG', # 全角・半角カッコ考慮
            '火力(石炭)': 'coal',
            '火力(石油)': 'oil',
            '火力(その他)': 'other_fire',
            '水力': 'hydro',
            '地熱': 'geothermal',
            'バイオマス': 'biomass',
            '太陽光発電実績': 'solar_actual', '太陽光実績': 'solar_actual',
            '太陽光出力制御量': 'solar_control', '太陽光制御量': 'solar_control',
            '風力発電実績': 'wind_actual', '風力実績': 'wind_actual',
            '風力出力制御量': 'wind_control', '風力制御量': 'wind_control',
            '揚水': 'pumped_storage',
            '蓄電池': 'battery',
            '連系線': 'interconnection',
            'その他': 'other',
            '合計': 'total'
        }

        rename_dict = {}
        used_standard_cols = set()
        original_columns_lower = [str(c).lower().strip() for c in df_data.columns]

        for standard_name in self.DEMAND_COLUMNS_ORDER:
            found = False
            for jp_key, en_key in column_mapping.items():
                if en_key == standard_name:
                    try:
                        # マッピングキー（日本語名など）が元の列名（小文字）に含まれるかチェック
                        col_index = original_columns_lower.index(jp_key)
                        original_col_name = df_data.columns[col_index]
                        if standard_name not in used_standard_cols:
                             rename_dict[original_col_name] = standard_name
                             used_standard_cols.add(standard_name)
                             found = True
                             break # 一致したら次のstandard_nameへ
                    except ValueError:
                        continue # 元の列名にマッピングキーがなければ次へ
            if not found:
                 logger.warning(f"標準カラム'{standard_name}'に対応する元カラムが見つかりませんでした。")

        df_renamed = df_data.rename(columns=rename_dict)
        logger.info(f"列名変更後 (マッピングされたもの): {list(rename_dict.values())}")

        # マッピングできなかった必須列がないか確認 (rename後の名前で確認)
        if 'date' not in df_renamed.columns or 'slot' not in df_renamed.columns:
             logger.error("必須の'date'または'slot'列(元TIME)が見つかりません。処理を中断します。")
             return pd.DataFrame()

        # --- 日付と時間の処理 (Rename後の列名を使用) ---
        # 日付を YYYYMMDD 文字列に変換 (元形式 YYYY/MM/DD)
        try:
             # ★ rename後の 'date' 列を使用
             df_renamed['date_str'] = pd.to_datetime(df_renamed['date'], format='%Y/%m/%d', errors='coerce').dt.strftime('%Y%m%d')
             df_renamed = df_renamed.dropna(subset=['date_str']) # 不正な日付を除外
             if df_renamed.empty: # dropna の結果、空になった場合
                  logger.error("日付変換後に有効なデータがありません。")
                  return pd.DataFrame()
        except Exception as e:
             # ★ エラーメッセージも rename後の列名を使うように修正
             logger.error(f"日付列 ('date') の変換エラー (YYYY/MM/DD -> YYYYMMDD): {e}", exc_info=True)
             return pd.DataFrame()

        # 時間をスロット番号 (1-48) に変換 (元形式 HH:MM)
        def time_to_slot(time_str):
             if pd.isna(time_str):
                 return 0
             try:
                 hour, minute = map(int, str(time_str).split(':'))
                 slot = hour * 2 + (1 if minute == 0 else 2) # 00:00 -> 1, 00:30 -> 2
                 return 48 if slot == 0 and hour == 24 else slot # 24:00 -> 48 考慮
             except Exception as e: # エラー内容もログに出力
                 logger.warning(f"不正な時間形式: '{time_str}' ({e})。 スロット0とします。")
                 return 0
        try:
            # ★ rename後の 'slot' 列を使用
            df_renamed['slot_num'] = df_renamed['slot'].apply(time_to_slot)
        except Exception as e:
            logger.error(f"スロット列('slot')の変換エラー: {e}", exc_info=True)
            return pd.DataFrame()

        # --- マスターキー生成 (YYYYMMDD_Slot) ---
        df_renamed['master_key'] = df_renamed.apply(
            lambda row: f"{row['date_str']}_{row['slot_num']}",
            axis=1
        )

        # --- 数値データの整形 と 結果DataFrameの作成 ---
        df_result = pd.DataFrame()
        df_result['master_key'] = df_renamed['master_key']
        # ★ date列は YYYYMMDD 文字列として保存
        df_result['date'] = df_renamed['date_str']
        df_result['slot'] = df_renamed['slot_num']

        # DEMAND_COLUMNS_ORDER に含まれる数値カラムを処理
        numeric_columns_expected = [col for col in self.DEMAND_COLUMNS_ORDER if col not in ['master_key', 'date', 'slot']]
        for col_std in numeric_columns_expected:
            if col_std in df_renamed.columns:
                 # 全角数字やカンマ除去、数値変換
                 df_result[col_std] = pd.to_numeric(df_renamed[col_std].astype(str).str.replace('[０-９]', lambda m: chr(ord(m.group(0)) - 0xFEE0), regex=True).str.replace(',', ''), errors='coerce')
                 df_result[col_std] = df_result[col_std].fillna(0) # NaNは0で埋める
            else:
                 logger.debug(f"標準数値カラム '{col_std}' がマッピングされなかったため、0で埋めます。")
                 df_result[col_std] = 0

        # 不要になった一時列や元の列を削除 (元の列名は rename_dict のキー)
        cols_to_drop = list(rename_dict.keys()) + ['date_str', 'slot_num']
        # df_result に含まれない列を削除しようとするとエラーになるので存在確認
        cols_to_drop_existing = [col for col in cols_to_drop if col in df_result.columns]
        df_result = df_result.drop(columns=cols_to_drop_existing, errors='ignore')

        # DEMAND_COLUMNS_ORDER に合わせて列を並び替え & 不足列を0で埋める
        df_result = df_result.reindex(columns=self.DEMAND_COLUMNS_ORDER, fill_value=0)

        # 重複マスターキーを削除 (念のため同一ファイル内での重複も除く)
        original_count = len(df_result)
        df_result = df_result.drop_duplicates(subset=['master_key'], keep='first')
        if len(df_result) < original_count:
            logger.warning(f"同一ファイル内で重複マスターキー {original_count - len(df_result)}件 を削除しました。")

        logger.info(f"需要データ整形完了: TSO={tso_id}, 最終Shape={df_result.shape}")
        return df_result

    def _process_supply_data(self, df: pd.DataFrame, target_date: date, tso_id: str) -> pd.DataFrame:
        """
        供給データを標準形式に処理。
        現在は需要データと同じ処理を呼び出す。
        unified_downloader.py から移植。
        """
        logger.info(f"供給データ処理開始 (需要データと同じ処理を使用): TSO={tso_id}, Date={target_date}")
        return self._process_demand_data(df, target_date, tso_id)

    def _set_column_names_for_chubu(self, df: pd.DataFrame, tso_id: str = 'chubu') -> pd.DataFrame:
        """
        中部電力のCSV/ZIP内CSVファイル用の整形処理。
        header=1 で読み込まれたDataFrameを前提とする。
        列名変換、データ型変換、master_key生成を行う。
        Args:
            df: header=1 で読み込まれたDataFrame
            tso_id: TSO ID (通常は 'chubu')
        Returns:
            DataFrame: 整形後のデータフレーム
        """
        logger.info(f"中部電力データ整形開始: TSO={tso_id}, 元Shape={df.shape}")
        if df.empty:
             logger.warning("中部電力: 入力データが空です。")
             return pd.DataFrame()

        # header=1 で読み込んでいる前提のため、df.columns がヘッダー
        df_data = df.copy()

        # 列名マッピング (中部電力専用だが、_process_demand_data とほぼ同じはず)
        column_mapping_chubu = {
            'date': 'date', 'time': 'slot',
            'エリア需要': 'area_demand',
            '原子力': 'nuclear',
            '火力(lng)': 'LNG', # 小文字・括弧考慮
            '火力(石炭)': 'coal',
            '火力(石油)': 'oil',
            '火力(その他)': 'other_fire',
            '水力': 'hydro',
            '地熱': 'geothermal',
            'バイオマス': 'biomass',
            '太陽光発電実績': 'solar_actual',
            '太陽光出力制御量': 'solar_control',
            '風力発電実績': 'wind_actual',
            '風力出力制御量': 'wind_control',
            '揚水': 'pumped_storage',
            '蓄電池': 'battery',
            '連系線': 'interconnection',
            'その他': 'other',
            '合計': 'total'
        }

        rename_dict = {}
        used_standard_cols = set()
        original_columns_lower = [str(c).lower().strip() for c in df_data.columns]

        for jp_key, standard_name in column_mapping_chubu.items():
             try:
                 # 元の列名を特定 (小文字で比較)
                 col_index = original_columns_lower.index(jp_key)
                 original_col_name = df_data.columns[col_index]
                 if standard_name not in used_standard_cols:
                      rename_dict[original_col_name] = standard_name
                      used_standard_cols.add(standard_name)
             except ValueError:
                 logger.warning(f"中部電力: マッピングキー '{jp_key}' が元の列名 '{original_columns_lower}' に見つかりません。")

        df_renamed = df_data.rename(columns=rename_dict)
        logger.info(f"中部電力: 列名変更後 (マッピングされたもの): {list(rename_dict.values())}")

        # マッピングできなかった必須列がないか確認 (rename後の名前で確認)
        if 'date' not in df_renamed.columns or 'slot' not in df_renamed.columns:
            logger.error("中部電力: 必須の 'date' または 'slot' 列のマッピングに失敗しました。")
            return pd.DataFrame()

        # --- 日付と時間の処理 (Rename後の列名を使用) ---
        # 日付変換 (YYYY/MM/DD -> YYYYMMDD)
        try:
            # errors='coerce' で不正な形式は NaT にする
            df_renamed['date_str'] = pd.to_datetime(df_renamed['date'], format='%Y/%m/%d', errors='coerce').dt.strftime('%Y%m%d')
            # NaT になった行を削除
            initial_rows = len(df_renamed)
            df_renamed = df_renamed.dropna(subset=['date_str'])
            if len(df_renamed) < initial_rows:
                logger.warning(f"中部電力: 不正な日付形式のため {initial_rows - len(df_renamed)} 行を削除しました。")
            if df_renamed.empty:
                 logger.error("中部電力: 有効な日付を持つ行がありません。")
                 return pd.DataFrame()
        except Exception as e:
            logger.error(f"中部電力: 日付列('date')変換中に予期せぬエラー: {e}", exc_info=True)
            return pd.DataFrame()

        # 時間変換 (HH:MM -> 1-48)
        def time_to_slot(time_str):
             if pd.isna(time_str):
                 return 0
             try:
                 # HH:MM 形式を想定
                 hour, minute = map(int, str(time_str).split(':'))
                 # 00:00 -> 1, 00:30 -> 2
                 slot = hour * 2 + (1 if minute == 0 else 2)
                 # 24:00 は 48 として扱う (もしあれば)
                 return 48 if slot == 0 and hour == 24 else slot
             except Exception as e:
                 logger.warning(f"中部電力: 不正な時間形式 '{time_str}' ({e})。 スロット0とします。")
                 return 0
        try:
            df_renamed['slot_num'] = df_renamed['slot'].apply(time_to_slot)
        except Exception as e:
             logger.error(f"中部電力: スロット列('slot')変換中に予期せぬエラー: {e}", exc_info=True)
             return pd.DataFrame()

        # マスターキー生成 (YYYYMMDD_Slot)
        try:
            df_renamed['master_key'] = df_renamed.apply(
                lambda row: f"{row['date_str']}_{row['slot_num']}",
                axis=1
            )
        except Exception as e:
             logger.error(f"中部電力: マスターキー生成中にエラー: {e}", exc_info=True)
             return pd.DataFrame()

        # --- 数値データの整形 と 結果DataFrameの作成 ---
        df_result = pd.DataFrame()
        df_result['master_key'] = df_renamed['master_key']
        df_result['date'] = df_renamed['date_str'] # YYYYMMDD
        df_result['slot'] = df_renamed['slot_num'] # 1-48

        numeric_columns_expected = [col for col in self.DEMAND_COLUMNS_ORDER if col not in ['master_key', 'date', 'slot']]
        for col_std in numeric_columns_expected:
            # rename後の列名(col_std)がdf_renamedに存在するかチェック
            if col_std in df_renamed.columns:
                 try:
                     # 全角数字やカンマ除去、数値変換
                     df_result[col_std] = pd.to_numeric(df_renamed[col_std].astype(str).str.replace('[０-９]', lambda m: chr(ord(m.group(0)) - 0xFEE0), regex=True).str.replace(',', ''), errors='coerce')
                     df_result[col_std] = df_result[col_std].fillna(0)
                 except Exception as e:
                      logger.warning(f"中部電力: 数値変換エラー ({col_std}): {e}。0で埋めます。")
                      df_result[col_std] = 0
            else:
                 # マッピングされなかった標準列は0
                 df_result[col_std] = 0

        # 列順序を標準に合わせる
        df_result = df_result.reindex(columns=self.DEMAND_COLUMNS_ORDER, fill_value=0)

        # 同一ファイル内での重複マスターキー削除
        initial_rows_final = len(df_result)
        df_result = df_result.drop_duplicates(subset=['master_key'], keep='first')
        if len(df_result) < initial_rows_final:
            logger.warning(f"中部電力: 同一ファイル内で重複マスターキー {initial_rows_final - len(df_result)}件 を削除しました。")

        logger.info(f"中部電力データ整形完了: 最終Shape={df_result.shape}")
        return df_result 