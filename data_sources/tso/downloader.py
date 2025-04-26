import requests
import logging
import re
from datetime import date, datetime
from typing import Union

# SSLの証明書検証警告を無効化（既存の unified_downloader.py から移動）
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

class TSODataDownloader:
    """
    TSOデータをダウンロードするためのクラス。
    URLの取得、リクエストの実行、特殊なケース（中部、東北）の処理を担当。
    """
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def download_content(self, url: str, **kwargs) -> bytes:
        """
        URLからコンテンツをバイナリ形式でダウンロードする。
        unified_downloader.py の _download_content を移植。
        Args:
            url: ダウンロード対象のURL
            **kwargs: リクエストの追加パラメータ (例: params)
        Returns:
            bytes: ダウンロードしたバイナリデータ
        Raises:
            ValueError: ダウンロードに失敗した場合
        """
        try:
            params = kwargs.get('params', {})
            logger.info(f"{url} からコンテンツをダウンロード中 (Params: {params})")
            response = requests.get(url, headers=self.headers, params=params, verify=False, timeout=60)
            response.raise_for_status()

            if len(response.content) == 0:
                logger.warning(f"空のレスポンスを受け取りました: {url}")
                # 空でも成功として扱う場合があるため、空のバイト列を返す
                return b''

            return response.content
        except requests.RequestException as e:
            logger.error(f"{url} からのダウンロードエラー: {str(e)}")
            raise ValueError(f"コンテンツのダウンロードに失敗しました: {str(e)}")

    def fetch_data(self, tso_id: str, base_url: str, target_date: date, **kwargs) -> Union[bytes, str]:
        """
        指定されたTSO ID、URL、日付に基づいてデータを取得する。
        unified_downloader.py の download_csv のロジックの一部を移植・統合。
        中部電力、東北電力の特殊なURL処理を行う。

        Args:
            tso_id: TSO ID
            base_url: ベースとなるURL（URLテンプレートから取得したもの）
            target_date: 対象日付
            **kwargs: 追加パラメータ (主に params)

        Returns:
            Union[bytes, str]: ダウンロードしたデータ（ZIPの場合はbytes、CSVの場合はstr）

        Raises:
            ValueError: データ取得に失敗した場合
        """
        url = base_url
        params = {
            'year': target_date.year,
            'month': target_date.month,
            'day': target_date.day
        }
        params.update(kwargs.get('params', {}))

        logger.info(f"データ取得試行: TSO={tso_id}, URL={url}, Date={target_date}")

        # --- 中部電力の特殊処理 --- (unified_downloader.py から移植)
        if tso_id == 'chubu' and '.zip' in url:
            current_year = datetime.now().year
            test_years = [target_date.year] # まず対象年
            if target_date.year > current_year:
                 test_years = [current_year] # 未来日は今年
            for offset in [-1, 1]: # 前後1年
                 alt_year = target_date.year + offset
                 if alt_year <= current_year and alt_year not in test_years:
                     test_years.append(alt_year)
            if current_year not in test_years: # 現在の年
                 test_years.append(current_year)

            logger.info(f"中部電力: 試行する年: {test_years}")
            original_url = url
            success = False
            for test_year in test_years:
                alt_url = re.sub(r'eria_jukyu_\d{4}\.zip', f'eria_jukyu_{test_year}.zip', original_url)
                logger.info(f"中部電力: {alt_url} を試行")
                try:
                    # ZIPファイルはバイナリで取得 (params不要)
                    content = self.download_content(alt_url)
                    if content: # 空でないバイナリが返れば成功
                        logger.info(f"中部電力 {test_year}年データ取得成功: {alt_url}")
                        # ZIPファイルはバイナリコンテンツを返す
                        return content
                    else:
                         logger.warning(f"中部電力 {test_year}年データ: 空のレスポンス")
                except ValueError as e:
                    # download_content内でログ出力済みなのでここでは警告レベル
                    logger.warning(f"中部電力 {test_year}年データ取得試行エラー: {str(e)}")
                except requests.RequestException as e:
                    logger.warning(f"中部電力 {test_year}年データ接続エラー: {str(e)}")

            logger.error(f"中部電力: 利用可能なZIPデータが見つかりませんでした。試行した年: {test_years}")
            raise ValueError(f"中部電力のZIPデータが見つかりませんでした。試行した年: {test_years}")

        # --- 東北電力の特殊処理 --- (unified_downloader.py から移植)
        elif tso_id == 'tohoku' and 'eria_jukyu_' in url:
            original_url = url
            version_numbers = ['01', '02', '03', '04', '05']
            success = False
            for version in version_numbers:
                alt_url = re.sub(r'_\d{2}\.csv$', f'_{version}.csv', original_url)
                logger.info(f"東北電力: 代替URL {alt_url} を試行")
                try:
                    # CSVはテキストで取得
                    content = self.download_content(alt_url, params=params)
                    # contentがbytesならテキストに変換してみる
                    if isinstance(content, bytes):
                        try:
                            # 簡単なUTF-8デコード試行
                            text_content = content.decode('utf-8')
                        except UnicodeDecodeError:
                             try:
                                 # Shift_JIS試行
                                 text_content = content.decode('shift-jis')
                             except UnicodeDecodeError:
                                 logger.warning(f"東北電力 {alt_url}: デコード失敗、バイナリのまま処理続行")
                                 text_content = content # デコード失敗時はbytesのまま
                    else:
                        text_content = content # 最初からstrの場合

                    # テキストの場合、中身があるか確認
                    if isinstance(text_content, str) and text_content.strip():
                        logger.info(f"東北電力: 有効なURL発見: {alt_url}")
                        return text_content # 有効なテキストコンテンツ
                    # バイナリの場合、中身があるか確認 (空でないか)
                    elif isinstance(text_content, bytes) and text_content:
                         logger.info(f"東北電力: 有効なURL発見 (バイナリ): {alt_url}")
                         return text_content # 有効なバイナリコンテンツ
                    else:
                        logger.warning(f"東北電力 {alt_url}: 空のレスポンス")

                except ValueError as e:
                    logger.warning(f"東北電力 {alt_url} 取得エラー: {str(e)}")
                except requests.RequestException as e:
                    logger.warning(f"東北電力 {alt_url} 接続エラー: {str(e)}")

            logger.error(f"東北電力: 利用可能なCSVデータが見つかりませんでした。試行URLパターン: {original_url.replace('.csv', '_XX.csv')}")
            raise ValueError(f"東北電力のCSVデータが見つかりませんでした")

        # --- その他のTSO または 特殊処理不要なケース --- 
        else:
            try:
                # ZIPかどうかをURLで判断 (中部以外でもZIPの可能性？)
                if '.zip' in url.lower():
                    # ZIPファイルはバイナリで取得 (params不要)
                    logger.info(f"ZIPファイルとしてダウンロード: {url}")
                    return self.download_content(url)
                else:
                    # CSVファイルはバイナリで取得し、後でパーサがデコード
                    logger.info(f"CSVファイルとしてダウンロード: {url}, Params: {params}")
                    return self.download_content(url, params=params)
            except ValueError as e:
                 logger.error(f"データ取得失敗: TSO={tso_id}, URL={url}, Error={e}")
                 raise
            except requests.RequestException as e:
                logger.error(f"接続エラー: TSO={tso_id}, URL={url}, Error={e}")
                raise ValueError(f"接続エラー: {e}") 