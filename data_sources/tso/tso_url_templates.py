import logging
from datetime import date, datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

# TSOエリア情報 (tso_urls.py から移植)
TSO_INFO: Dict[str, Dict[str, Any]] = {
    "hokkaido": {
        "id": "hokkaido",
        "name": "北海道電力",
        "area_code": "1",
        "region": "北海道",
        # URLはget_tso_url関数で動的に生成するため、テンプレートは削除またはコメントアウトしても良い
        # "demand_url_template": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{YYYY}{MM}_01.csv",
        # "supply_url_template": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{YYYY}{MM}_01.csv"
    },
    "tohoku": {
        "id": "tohoku",
        "name": "東北電力",
        "area_code": "2",
        "region": "東北",
        # "demand_url_template": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{YYYY}{MM}_02.csv",
        # "supply_url_template": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{YYYY}{MM}_02.csv"
    },
    "tepco": {
        "id": "tepco",
        "name": "東京電力",
        "area_code": "3",
        "region": "関東",
        # "demand_url_template": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{YYYY}{MM}_03.csv",
        # "supply_url_template": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{YYYY}{MM}_03.csv"
    },
    "chubu": {
        "id": "chubu",
        "name": "中部電力",
        "area_code": "4",
        "region": "中部",
        # "demand_url_template": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{YYYY}.zip",
        # "supply_url_template": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{YYYY}.zip"
    },
    "hokuriku": {
        "id": "hokuriku",
        "name": "北陸電力",
        "area_code": "5",
        "region": "北陸",
        # "demand_url_template": "https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{YYYY}{MM}.csv", # tso_urls.py と若干違うが get_tso_urlに合わせる
        # "supply_url_template": "https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{YYYY}{MM}.csv"
    },
    "kansai": {
        "id": "kansai",
        "name": "関西電力",
        "area_code": "6",
        "region": "関西",
        # "demand_url_template": "https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{YYYY}{MM}.csv", # tso_urls.py と違うが get_tso_urlに合わせる
        # "supply_url_template": "https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{YYYY}{MM}.csv"
    },
    "chugoku": {
        "id": "chugoku",
        "name": "中国電力",
        "area_code": "7",
        "region": "中国",
        # "demand_url_template": "https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{YYYY}{MM}.csv", # tso_urls.py と違うが get_tso_urlに合わせる
        # "supply_url_template": "https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{YYYY}{MM}.csv"
    },
    "shikoku": {
        "id": "shikoku",
        "name": "四国電力",
        "area_code": "8",
        "region": "四国",
        # "demand_url_template": "https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{YYYY}{MM}_jukyu.csv",
        # "supply_url_template": "https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{YYYY}{MM}_jukyu.csv"
    },
    "kyushu": {
        "id": "kyushu",
        "name": "九州電力",
        "area_code": "9",
        "region": "九州",
        # "demand_url_template": "https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{YYYY}{MM}.csv",
        # "supply_url_template": "https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{YYYY}{MM}.csv"
    },
    "okinawa": {
        "id": "okinawa",
        "name": "沖縄電力",
        "area_code": "10", # tso_urls.py では area_code 10 だった
        "region": "沖縄",
        # "demand_url_template": "https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{YYYY}{MM}.csv", # tso_urls.py と違うが get_tso_urlに合わせる
        # "supply_url_template": "https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{YYYY}{MM}.csv"
    }
}

# 各TSOごとのURL形式を定義 (get_tso_url の内部で使用)
# これは TSO_INFO とは別に保持し、get_tso_url で動的に生成する
# 注意: TSO_INFO内のURLテンプレートはコメントアウトまたは削除しました
_TSO_URL_FORMATS = {
    "hokkaido": {
        "demand": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{year_month}_01.csv",
        "supply": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{year_month}_01.csv"
    },
    "tohoku": {
        "demand": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{year_month}_02.csv",
        "supply": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{year_month}_02.csv"
    },
    "tepco": {
        "demand": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{year_month}_03.csv",
        "supply": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{year_month}_03.csv"
    },
    "chubu": {
        "demand": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{year}.zip",
        "supply": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{year}.zip"
    },
    "hokuriku": {
        "demand": "https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{year_month}.csv",
        "supply": "https://www.rikuden.co.jp/nw_jyukyuu/csv/area_{year_month}.csv"
    },
    "kansai": {
        "demand": "https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{year_month}.csv",
        "supply": "https://www.kansai-td.co.jp/yamasou/juyo-jisseki/jisseki/ji_{year_month}.csv"
    },
    "chugoku": {
        "demand": "https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{year_month}.csv",
        "supply": "https://www.energia.co.jp/nw/service/supply/juyo/sys/juyo-jisseki-{year_month}.csv"
    },
    "shikoku": {
        "demand": "https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{year_month}_jukyu.csv",
        "supply": "https://www.yonden.co.jp/nw/assets/renewable_energy/data/download_juyo/{year_month}_jukyu.csv"
    },
    "kyushu": {
        "demand": "https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{year_month}.csv",
        "supply": "https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure-area-performance_{year_month}.csv"
    },
    "okinawa": {
        "demand": "https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{year_month}.csv",
        "supply": "https://www.okiden.co.jp/td-service/renewable-energy/supply_demand/csv/area_jokyo_{year_month}.csv"
    }
}

# target_date を受け取り、それに基づいてURLを動的に生成する関数
def get_tso_url(tso_id: str, url_type: str, target_date: date) -> str:
    """
    指定されたTSO ID、URLタイプ、日付に基づいてURLを取得します。
    内部の _TSO_URL_FORMATS を使用します。

    Args:
        tso_id: TSO ID (例: 'tepco', 'chubu')
        url_type: データの種類 ('demand' または 'supply')
        target_date: 対象日付

    Returns:
        URL文字列

    Raises:
        ValueError: 無効なTSO IDまたはURLタイプの場合
    """
    if target_date is None:
        target_date = datetime.now().date()

    year = target_date.year
    month = target_date.month
    year_month = f"{year}{month:02d}"

    # TSO_INFO ではなく _TSO_URL_FORMATS を参照
    if tso_id not in _TSO_URL_FORMATS:
        logger.error(f"無効なTSO ID: {tso_id}")
        raise ValueError(f"無効なTSO ID: {tso_id}")

    if url_type not in _TSO_URL_FORMATS[tso_id]:
        logger.error(f"TSO {tso_id} に対してURL種別 {url_type} はサポートされていません")
        raise ValueError(f"TSO {tso_id} に対してURL種別 {url_type} はサポートされていません")

    # フォーマット文字列を取得
    url_format = _TSO_URL_FORMATS[tso_id][url_type]

    # プレースホルダーを置換
    try:
        # f-string ではなく .format() を使用して置換
        url = url_format.format(year=year, year_month=year_month)
    except KeyError as e:
        logger.error(f"URLフォーマット置換中にエラー ({url_format}): キー {e} が見つかりません")
        raise ValueError(f"URLフォーマットの置換に失敗 ({url_format}): {e}")

    logger.info(f"取得したURL ({tso_id}, {url_type}, {target_date}): {url}")
    return url

# 利用可能なTSO IDのリスト (TSO_INFOのキーと一致)
VALID_TSO_IDS = list(TSO_INFO.keys())

# --- tso_urls.py から移植したヘルパー関数 ---

def get_area_code_from_tso_id(tso_id: str) -> str:
    """
    TSO IDに対応するエリアコードを取得
    Args:
        tso_id: TSO ID
    Returns:
        エリアコード文字列
    Raises:
        ValueError: 無効なTSO IDが指定された場合
    """
    if tso_id not in TSO_INFO:
        raise ValueError(f"無効なTSO ID: {tso_id}")
    return TSO_INFO[tso_id]["area_code"]

def get_tso_id_from_area_code(area_code: str) -> str:
    """
    エリアコードに対応するTSO IDを取得
    Args:
        area_code: エリアコード
    Returns:
        TSO ID
    Raises:
        ValueError: 無効なエリアコードが指定された場合
    """
    area_code_str = str(area_code).strip() # 文字列化と比較のため空白除去
    for tso_id, info in TSO_INFO.items():
        if info.get("area_code") == area_code_str:
            return tso_id
    raise ValueError(f"無効なエリアコード: {area_code}") 