#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCCTO 連系線空容量データ取得スクリプト (改訂版 v4 - GET First, 2-step DL)

OCCTO 公開サイトから指定された対象日の連系線空容量データ（当日タブ相当）を取得し、
CSV ファイルとして保存します。

処理フロー:
1. データページにGETアクセスし、HTMLから初期トークンを取得。
2. actionSubType='print' でPOSTリクエストを送信し、downloadKeyと更新トークンを取得。
3. /public/common/fileDownload にGETアクセスし、CSV本体をダウンロード。

実行前に:
pip install requests lxml pandas

使い方例:
# 本日のデータを取得 (策定日は自動で前日)
python data_sources/occto/intertie_capacity.py

# 特定の日付のデータを取得
python data_sources/occto/intertie_capacity.py --target-date 2025-05-01 --plan-date 2025-04-30

# 出力ファイル名を指定
python data_sources/occto/intertie_capacity.py --target-date 2025-05-01 --out data/occto_capacity_20250501.csv
"""

import argparse
import sys
import time
import requests
import pandas as pd
from datetime import date, timedelta, datetime
from urllib.parse import urlencode
from typing import Optional, Tuple
import json
import os
import re
import logging
import lxml.html as LH
from io import StringIO # CSV文字列をpandasに渡すため

# --- ロギング設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr # 標準エラー出力にログを出す
)

# --- 定数定義 ---
BASE_URL = "https://occtonet3.occto.or.jp"
# 1. トークン取得用のエントリーポイントURL
ENTRY_URL = (
    f"{BASE_URL}/public/dfw/RP11/OCCTO/SD/CA01S013C"
    "?fwExtention.pathInfo=CA01S013C&fwExtention.prgbrh=0"
)
# 2. printリクエスト用POST URL
POST_URL = f"{BASE_URL}/public/dfw/RP11/OCCTO/SD/CA01S013C"
# 3. CSVダウンロード用GET URL Base
DOWNLOAD_URL_BASE = f"{BASE_URL}/public/common/fileDownload"

REQUEST_HEADERS = {
    # User-AgentはSessionに設定
    'X-Requested-With': 'XMLHttpRequest',
}
REQUEST_WAIT_SECONDS = 1.0 # 待機時間

# 固定パラメータ: HAR で常に Y / 2 だったもの

COMMON_PAYLOAD_EXTRA = {
    # 固定パラメータ（HAR で常に Y / 2 だったもの）
    'dvlSlashLblUpdaf': '2',
    'hukuKbnCd': '2',
    'allChk1': 'Y',
    # 連系線13本全部オン
    **{f"rkl{str(i).zfill(2)}": 'Y' for i in range(1, 14)},
}

# --- ユーティリティ関数 ---

def _validate_date_str(date_str: str) -> date:
    """日付文字列 (YYYY-MM-DD) を検証し、date オブジェクトを返す"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"日付の形式が不正です: {date_str} (YYYY-MM-DD 形式で指定してください)")

def _wait_before_request(seconds: float = REQUEST_WAIT_SECONDS) -> None:
    """リクエスト前に指定秒数待機する"""
    if seconds > 0:
        logging.info(f"待機中 ({seconds:.1f}秒)...")
    time.sleep(seconds)

def strip_xssi(txt: str) -> str:
    """先頭に ])}' などの XSSI 対策文字列が付いていたら除去"""
    # BOM(U+FEFF)も考慮
    cleaned = re.sub(r"^\s*\ufeff*\)\}\'s*,\\s*", "", txt)
    # logging.debug(f"XSSI strip: original starts with {txt[:10]}, cleaned starts with {cleaned[:10]}")\n    return cleaned
    return cleaned

def safe_json(response: requests.Response, context: str = "応答") -> Optional[dict]:
    """XSSI除去とエラーハンドリング付きでJSONをパースする"""
    txt_raw = response.text
    logging.debug(f"{context} raw先頭100→ {repr(txt_raw[:100])}") # contextを使用
    txt = strip_xssi(txt_raw)
    logging.debug(f"{context} cleaned先頭100→ {repr(txt[:100])}") # contextを使用
    if not txt:
        logging.warning(f"{context} JSONパース試行: レスポンスボディが空です。")
        return None
    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        logging.error(f"{context} JSONパースエラー: {e}")
        logging.error(f"{context} レスポンス内容(先頭500文字):\\n{txt[:500]}")
        return None

# --- 主要処理関数 ---

def fetch_tokens(sess: requests.Session) -> Tuple[str, str]:
    """
    エントリーポイントURLにGETアクセスし、HTMLから初期トークンを試行的に取得する。
    取得できなくてもエラーとせず、空文字列を返す。
    """
    logging.info(f"初期トークン取得試行 (GET: {ENTRY_URL})...")
    ajax_token = ""
    request_token = ""
    try:
        _wait_before_request(0.5) # 短めの待機
        response = sess.get(ENTRY_URL, timeout=30)
        response.raise_for_status() # エラーがあれば例外発生
        # 文字コードを推定してデコード
        response.encoding = response.apparent_encoding
        html_content = response.text
        logging.info("HTMLからトークン抽出試行...")

        try:
            doc = LH.fromstring(html_content)
        except Exception as e:
            logging.warning(f"HTMLのパースに失敗しました: {e}")
            logging.debug(f"パース失敗時のHTML内容(先頭1000文字):\\n{html_content[:1000]}")
            # パース失敗でも処理を継続するため空トークンを返す
            return ajax_token, request_token

        # XPathを使用して隠しフィールドを抽出
        # まずは特定のフォームを試す
        xpath_expr_form = '//form[@id="CA01S013P"]//input[@type="hidden" and @name]'
        hidden_inputs = doc.xpath(xpath_expr_form)

        if not hidden_inputs:
            logging.debug(f"XPath '{xpath_expr_form}' でHiddenフィールド見つからず。より緩い条件で再試行。")
            # フォーム指定なしで再試行
            xpath_expr_all = '//input[@type="hidden" and @name]'
            hidden_inputs = doc.xpath(xpath_expr_all)
            used_xpath = xpath_expr_all
        else:
            used_xpath = xpath_expr_form

        tokens = {i.name: i.value for i in hidden_inputs if i.name and i.value} # nameとvalueがあるもののみ
        logging.debug(f"見つかったHiddenフィールド (XPath: '{used_xpath}'): {list(tokens.keys())}")

        ajax_token = tokens.get("ajaxToken", "") # なければ空文字
        request_token = tokens.get("requestToken", "") # なければ空文字

        if ajax_token and request_token:
            logging.info(f"初期トークン取得成功(試行): ajax={ajax_token[:10]}..., request={request_token[:10]}...")
        elif ajax_token or request_token:
             logging.warning(f"初期トークンの一部のみ取得(試行): ajax={'OK' if ajax_token else 'NG'}, request={'OK' if request_token else 'NG'}")
        else:
            logging.warning("HTMLから初期トークン(ajaxToken/requestToken)は見つかりませんでした。")
            # 見つからなかった場合もHTMLをログに出力（デバッグ用）
            logging.debug(f"トークンが見つからなかったHTML内容(先頭1000文字):\\n{html_content[:1000]}")

    except requests.exceptions.RequestException as e:
        # ネットワークエラー等
        logging.warning(f"初期トークン取得のためのGETリクエストに失敗しました: {e}", exc_info=True)
        # この場合も空トークンを返す（後続処理で失敗する可能性が高いが、処理は止めない）
    except Exception as e:
        # その他の予期せぬエラー
        logging.warning(f"初期トークン取得中に予期せぬエラーが発生しました: {e}", exc_info=True)
        # この場合も空トークンを返す

    # どんな状況でも取得できたトークン（または空文字列）を返す
    return ajax_token, request_token

def obtain_downloadkey_and_token(sess: requests.Session,
                                 target_occto: str,
                                 plan_occto: str,
                                 # ★ 追加: 初期トークンを受け取る
                                 initial_ajax_token: str = "",
                                 initial_request_token: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    print -> ok のシーケンスを実行し、ok 応答から downloadKey と更新された requestToken を取得する。
    初期トークンが提供された場合はそれを使用する。
    """
    logging.info("downloadKey/token 取得シーケンス開始 (print -> ok)...")

    # --- 1. print リクエスト ---
    logging.info("ステップ1/2: print リクエスト送信中...")
    payload_print = {
        "fwExtention.actionType": "reference", # HARに基づく
        "fwExtention.actionSubType": "print", # ★ print を指定
        "fwExtention.pathInfo": "CA01S013C",
        "fwExtention.prgbrh": "0",
        "fwExtention.formId": "CA01S013P",
        "transitionContextKey": "DEFAULT",
        "yokYokDayTdDnmKbn": "7", # 当日 (Fixed value from HAR?)
        "dvlDayFrom": plan_occto,
        "dvlDayTo": plan_occto,
        "tgtKknFrom": target_occto,
        "tgtKknTo": target_occto,
        # ★ 初期トークンを使用 (なければ空文字)
        "ajaxToken": initial_ajax_token,
        "requestToken": initial_request_token,
        "requestTokenBk": initial_request_token, # バックアップも同じ？要確認
    }
    payload_print.update(COMMON_PAYLOAD_EXTRA)

    headers_print = sess.headers.copy()
    headers_print.update({
        "sdReqType": "AJAX", # HARに基づく
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": ENTRY_URL, # 直前のGETリクエストのURL
    })

    _wait_before_request()
    try:
        response_print = sess.post(POST_URL, data=payload_print, headers=headers_print, timeout=30)
        response_print.raise_for_status()
        logging.info(f"print リクエスト成功 (ステータス: {response_print.status_code})")
        # print 応答はキーを含まないので、内容は無視するか、デバッグ用にログ出力する程度
        logging.debug(f"print 応答ボディ(無視対象): {safe_json(response_print, 'print応答')}")

    except requests.exceptions.RequestException as e:
        logging.error(f"print リクエストに失敗しました: {e}", exc_info=True)
        return None, None # シーケンス失敗

    # --- 2. ok リクエスト ---
    logging.info("ステップ2/2: ok リクエスト送信中 (downloadKey/token取得)...")
    payload_ok = {
        "fwExtention.actionType": "reference", # HARに基づく
        "fwExtention.actionSubType": "ok", # ★ ok を指定
        "fwExtention.pathInfo": "CA01S013C",
        "fwExtention.prgbrh": "0",
        "fwExtention.formId": "CA01S013P",
        "transitionContextKey": "DEFAULT",
        "yokYokDayTdDnmKbn": "7",
        "dvlDayFrom": plan_occto,
        "dvlDayTo": plan_occto,
        "tgtKknFrom": target_occto,
        "tgtKknTo": target_occto,
        # ★ print と同じ初期トークンを使用 (なければ空文字)
        "ajaxToken": initial_ajax_token,
        "requestToken": initial_request_token,
        "requestTokenBk": initial_request_token,
    }
    payload_ok.update(COMMON_PAYLOAD_EXTRA)

    headers_ok = headers_print # print と同じヘッダで良いはず

        _wait_before_request()
    try:
        response_ok = sess.post(POST_URL, data=payload_ok, headers=headers_ok, timeout=30)
        response_ok.raise_for_status()
        logging.info(f"ok リクエスト成功 (ステータス: {response_ok.status_code})")

        # ok 応答からキーを抽出
        json_data = safe_json(response_ok, "ok応答")
        if json_data is None:
            logging.error("ok 応答の JSON パースに失敗しました。")
            return None, None

        # bizRoot と header の存在チェックを追加
        biz_root = json_data.get("bizRoot")
        if biz_root is None:
            logging.error("ok 応答 JSON に 'bizRoot' が存在しません。")
            logging.debug(f"ok 応答 JSON 全体: {json_data}")
            return None, None

        header = biz_root.get("header")
        if header is None:
            # header が null の場合も考慮
            logging.error("ok 応答 JSON の 'bizRoot' 内に 'header' が存在しないか、null です。")
            logging.debug(f"ok 応答 JSON (bizRoot): {biz_root}")
            return None, None

        download_key = header.get("downloadKey")
        # requestToken は bizRoot 直下にある可能性も？ HAR を再確認 -> header 直下のはず
        request_token_2 = header.get("requestToken")

        if not download_key or not request_token_2:
            logging.error("ok 応答 JSON から downloadKey または requestToken が見つかりませんでした。")
            logging.debug(f"ok 応答ヘッダ内容: {header}")
            return None, None

        logging.info(f"downloadKey と 更新 requestToken 取得成功: key={download_key[:10]}..., token={request_token_2[:10]}...")
        return download_key, request_token_2

    except requests.exceptions.RequestException as e:
        logging.error(f"ok リクエストに失敗しました: {e}", exc_info=True)
        return None, None # シーケンス失敗
    except Exception as e:
        logging.error(f"ok 応答の処理中に予期せぬエラー: {e}", exc_info=True)
        return None, None

def download_csv(sess: requests.Session, download_key: str, request_token_2: str) -> Optional[str]:
    """
    downloadKey と requestToken を使用して CSV データをダウンロードする。
    """
    logging.info("CSV データダウンロード中...")
    params = {
        "fwExtention.downloadKey": download_key,
        "fwExtention.requestToken": request_token_2, # ★ 更新されたトークンを使用
    }
    download_url = f"{DOWNLOAD_URL_BASE}?{urlencode(params)}"
    logging.debug(f"CSVダウンロードURL (パラメータ付き): {download_url}")

    _wait_before_request()
    try:
        response = sess.get(download_url, timeout=60) # ダウンロードは少し長めに待つ
        response.raise_for_status()
        response.encoding = 'cp932' # SHIFT-JIS (CP932) が期待される
        csv_content = response.text
        logging.info("CSV データのダウンロード成功。")
        # BOMがあれば除去 (pandasは通常ハンドルするが念のため)
        if csv_content.startswith('\\ufeff'):
            csv_content = csv_content[1:]
        logging.debug(f"ダウンロードしたCSV内容(先頭300文字):\\n{csv_content[:300]}")
        return csv_content
    except requests.exceptions.RequestException as e:
        logging.error(f"CSV ダウンロードリクエストに失敗しました: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"CSV ダウンロードまたは処理中に予期せぬエラー: {e}", exc_info=True)
        return None

def save_csv_data(csv_content: str, output_path: str) -> bool:
    """
    取得したCSV(文字列)をpandas DataFrame経由でファイルに保存する。
    """
    logging.info(f"CSVファイル保存処理開始 ({output_path})...")
    try:
        # StringIOを使って文字列をファイルのように扱う
        # 空文字列の場合はEmptyDataErrorになるはず
        df = pd.read_csv(StringIO(csv_content))

        if df.empty:
            logging.warning(f"CSVデータが0件です。ファイルは作成されますが空になります: {output_path}")
        else:
            logging.info(f"CSVデータの読み込み成功 ({len(df)}行)")

        # ディレクトリ作成
        output_dir = os.path.dirname(output_path)
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                logging.debug(f"出力ディレクトリを確認/作成しました: {output_dir}")
            except OSError as e:
                logging.error(f"出力ディレクトリの作成に失敗しました: {output_dir} - {e}", exc_info=True)
                return False

        # CSV 保存 (utf-8-sig: BOM付きUTF-8、Excel互換性のため)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"保存成功 → {output_path}")
        return True

    except pd.errors.EmptyDataError:
        # read_csvが空データでエラーを出した場合
        logging.warning(f"CSVデータが空またはヘッダーのみでした。空のファイルを作成します: {output_path}")
        # 空ファイルを作成する処理
        try:
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                pass # 空ファイル作成
            logging.info(f"空ファイル保存成功 → {output_path}")
            return True
        except Exception as e:
            logging.error(f"空ファイルの作成に失敗しました: {e}", exc_info=True)
            return False
    except Exception as e:
        logging.error(f"CSVの解析または保存中にエラーが発生しました: {e}", exc_info=True)
        logging.debug(f"渡されたCSV内容(最初の500文字):n{csv_content[:500]}")
        return False

def main():
    """スクリプトのメイン処理"""
    logging.info("OCCTO 連系線空容量データ取得スクリプト (v4) 開始...")
    parser = argparse.ArgumentParser(
        description='OCCTO 連系線空容量データを取得し CSV に保存します。',
        formatter_class=argparse.RawDescriptionHelpFormatter # ヘルプの改行を保持
    )
    parser.add_argument(
        '--target-date',
        type=_validate_date_str,
        default=None,
        metavar='YYYY-MM-DD',
        help='対象日 (未指定時は本日)'
    )
    parser.add_argument(
        '--plan-date',
        type=_validate_date_str,
        default=(date.today() - timedelta(days=1)), # デフォルトは昨日
        help='策定等日付 (YYYY-MM-DD)。デフォルトは前日。'
    )
    parser.add_argument(
        '--out',
        type=str,
        default=None,
        metavar='FILE_PATH',
        help='出力 CSV ファイルパス (未指定時は rkl_YYYY-MM-DD.csv)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='デバッグログを有効にする'
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("デバッグログが有効になりました。")

    # --- 日付設定 ---
    target_date: date
    if args.target_date:
        target_date = args.target_date
    else:
        target_date = date.today()
        logging.info(f"--target-date 未指定のため、本日 ({target_date.strftime('%Y-%m-%d')}) を対象とします。")

    plan_date: date
    if args.plan_date:
        plan_date = args.plan_date
    else:
        plan_date = date.today() - timedelta(days=1)
        logging.info(f"--plan-date 未指定のため、前日 ({plan_date.strftime('%Y-%m-%d')}) を策定日とします。")

    target_date_occto = target_date.strftime('%Y/%m/%d')
    plan_date_occto = plan_date.strftime('%Y/%m/%d')

    # --- 出力パス設定 ---
    output_path: str
    if args.out:
        output_path = args.out
    else:
        output_path = f"rkl_{target_date.strftime('%Y-%m-%d')}.csv"
        logging.info(f"--out 未指定のため、出力ファイルは {output_path} となります。")

    # --- メイン処理 ---
    exit_code = 1 # 失敗をデフォルトとする
    with requests.Session() as session:
        # User-Agentを設定
        session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'}) # User-Agent例
        logging.debug(f"Session User-Agent: {session.headers['User-Agent']}")

        try:
            # ステップ1: Cookie 取得 (GET)
            logging.info("\n--- ステップ1: Cookie 取得 (GET) ---")
            _wait_before_request(0.5)
            session.get(ENTRY_URL, timeout=20)

            # ステップ2: print で downloadKey / token 取得 (旧ステップ1, 2を統合)
            logging.info("\n--- ステップ1: print で downloadKey/token 取得 ---")
            download_key, request_token = obtain_downloadkey_and_token(
                session, target_date_occto, plan_date_occto
            )
            if not download_key or not request_token:
                logging.error("downloadKey または requestToken の取得に失敗しました。処理を終了します。")
            sys.exit(1)

            # ステップ3: CSV本体ダウンロード (旧ステップ3)
            logging.info("\n--- ステップ2: CSV本体ダウンロード (GET) ---")
            csv_content = download_csv(session, download_key, request_token)

            if csv_content is not None:
                # ステップ4: CSV保存 (旧ステップ4)
                logging.info("\n--- ステップ3: CSV保存 ---")
                if save_csv_data(csv_content, output_path):
                    exit_code = 0
            else:
                logging.error("CSVダウンロード失敗")

        except Exception as e:
            logging.error(f"メイン処理中に予期せぬエラーが発生しました: {e}", exc_info=True)
            exit_code = 1
    finally:
            logging.info("\nスクリプト終了。")

    sys.exit(exit_code)

if __name__ == "__main__":
    # 依存関係チェック (lxml)
    try:
        import lxml
    except ImportError:
        logging.error("\n*** 注意: 依存ライブラリ lxml が見つかりません。 ***")
        logging.error("*** pip install lxml を実行してください。 ***\n")
        sys.exit(1)
    main() 

# --- 旧コード (コメントアウト) ---
# def fetch_ajax_tokens(...): ...
# def query_print_and_parse(...): ...
# def fetch_initial_tokens(...): ...
# def fetch_and_save_csv(...): ...
# ... (その他の古い関数コメント) ... 