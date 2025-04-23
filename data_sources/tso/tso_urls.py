#!/usr/bin/env python3
"""
日本の電力会社（TSO）に関するURLとメタデータ

このモジュールは、日本の各電力会社（TSO）のデータダウンロード用URLと
各エリアに関する情報を提供します。
"""

import logging
from datetime import date
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# TSO ID: 電力会社コード
TSO_IDS = [
    "hokkaido", "tohoku", "tepco", "chubu", "hokuriku", "kansai", "chugoku", "shikoku", "kyushu", "okinawa"
]

# TSOエリア情報
TSO_INFO = {
    "hokkaido": {
        "id": "hokkaido", 
        "name": "北海道電力", 
        "area_code": "1",
        "region": "北海道",
        "demand_url": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{YYYY}{MM}_01.csv",
        "supply_url": "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{YYYY}{MM}_01.csv"
    },
    "tohoku": {
        "id": "tohoku", 
        "name": "東北電力", 
        "area_code": "2",
        "region": "東北",
        "demand_url": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{YYYY}{MM}_02.csv",
        "supply_url": "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{YYYY}{MM}_02.csv"
    },
    "tepco": {
        "id": "tepco", 
        "name": "東京電力", 
        "area_code": "3",
        "region": "関東",
        "demand_url": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{YYYY}{MM}_03.csv",
        "supply_url": "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{YYYY}{MM}_03.csv"
    },
    "chubu": {
        "id": "chubu", 
        "name": "中部電力", 
        "area_code": "4",
        "region": "中部",
        "demand_url": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{YYYY}.zip",
        "supply_url": "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{YYYY}.zip"
    },
    "hokuriku": {
        "id": "hokuriku", 
        "name": "北陸電力", 
        "area_code": "5",
        "region": "北陸",
        "demand_url": "https://www.rikuden.co.jp/nw/denki-yoho/csv/eria_jukyu_{YYYY}{MM}_05.csv",
        "supply_url": "https://www.rikuden.co.jp/nw/denki-yoho/csv/eria_jukyu_{YYYY}{MM}_05.csv"
    },
    "kansai": {
        "id": "kansai", 
        "name": "関西電力", 
        "area_code": "6",
        "region": "関西",
        "demand_url": "https://www.kansai-td.co.jp/yamasou/{YYYY}{MM}_jisseki.zip",
        "supply_url": "https://www.kansai-td.co.jp/yamasou/{YYYY}{MM}_jisseki.zip"
    },
    "chugoku": {
        "id": "chugoku", 
        "name": "中国電力", 
        "area_code": "7",
        "region": "中国",
        "demand_url": "https://www.energia.co.jp/nw/jukyuu/sys/eria_jukyu_{YYYY}{MM}_07.csv",
        "supply_url": "https://www.energia.co.jp/nw/jukyuu/sys/eria_jukyu_{YYYY}{MM}_07.csv"
    },
    "shikoku": {
        "id": "shikoku", 
        "name": "四国電力", 
        "area_code": "8",
        "region": "四国",
        "demand_url": "https://www.yonden.co.jp/nw/supply_demand/csv/eria_jukyu_{YYYY}{MM}_08.csv",
        "supply_url": "https://www.yonden.co.jp/nw/supply_demand/csv/eria_jukyu_{YYYY}{MM}_08.csv"
    },
    "kyushu": {
        "id": "kyushu", 
        "name": "九州電力", 
        "area_code": "9",
        "region": "九州",
        "demand_url": "https://www.kyuden.co.jp/td_area_jukyu/csv/eria_jukyu_{YYYY}{MM}_09.csv",
        "supply_url": "https://www.kyuden.co.jp/td_area_jukyu/csv/eria_jukyu_{YYYY}{MM}_09.csv"
    },
    "okinawa": {
        "id": "okinawa", 
        "name": "沖縄電力", 
        "area_code": "10",
        "region": "沖縄",
        "demand_url": "https://www.okiden.co.jp/denki2/eria_jukyu_{YYYY}{MM}_10.csv",
        "supply_url": "https://www.okiden.co.jp/denki2/eria_jukyu_{YYYY}{MM}_10.csv"
    }
}

def get_tso_url(tso_id: str, url_type: str = 'demand', target_date: Optional[date] = None) -> str:
    """
    指定されたTSO IDとURLタイプに対応するURLを取得
    
    Args:
        tso_id: TSO ID (例: 'tepco', 'hokuriku')
        url_type: URLタイプ ('demand'または'supply')
        target_date: 対象日付。URLのプレースホルダーを置換するために使用
        
    Returns:
        URL文字列
        
    Raises:
        ValueError: 無効なTSO IDまたはURLタイプが指定された場合
    """
    if tso_id not in TSO_INFO:
        raise ValueError(f"無効なTSO ID: {tso_id}。有効なID: {list(TSO_INFO.keys())}")
    
    if url_type not in ['demand', 'supply']:
        raise ValueError(f"無効なURLタイプ: {url_type}。'demand'または'supply'を指定してください")
    
    # URLの取得
    url_key = f"{url_type}_url"
    if url_key not in TSO_INFO[tso_id]:
        logger.error(f"TSO {tso_id} に {url_type} タイプのURLが定義されていません")
        raise ValueError(f"TSO {tso_id} に {url_type} タイプのURLが定義されていません")
    
    url = TSO_INFO[tso_id][url_key]
    
    # 日付が指定されている場合、URLにあるプレースホルダーを置換
    if target_date:
        try:
            # {YYYY} -> 年、{MM} -> 月のフォーマットで置換
            url = url.replace('{YYYY}', str(target_date.year))
            url = url.replace('{MM}', f"{target_date.month:02d}")  # 2桁の月
        except Exception as e:
            logger.error(f"URL置換中にエラーが発生しました: {str(e)}")
            raise ValueError(f"URLの日付プレースホルダー置換に失敗しました: {str(e)}")
    
    return url

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
    for tso_id, info in TSO_INFO.items():
        if info["area_code"] == str(area_code):
            return tso_id
    
    raise ValueError(f"無効なエリアコード: {area_code}") 