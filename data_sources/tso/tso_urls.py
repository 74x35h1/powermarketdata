#!/usr/bin/env python3
"""
TSO (送電系統運用者) URLとエリア情報の管理モジュール

このモジュールは、日本の電力会社（TSO）のURLやエリア情報を管理する機能を提供します。
設定ファイルからURLを読み込み、適切なフォーマットで提供します。
"""

import os
import sys
import json
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 有効なTSO IDs
VALID_TSO_IDS = [
    'hokkaido',
    'tohoku', 
    'tepco', 
    'chubu', 
    'hokuriku', 
    'kepco', 
    'chugoku', 
    'shikoku', 
    'kyushu'
]

# TSO情報の辞書
TSO_INFO: Dict[str, Dict[str, str]] = {
    'hokkaido': {
        'name': 'Hokkaido Electric Power Company',
        'area_code': '01',
        'region': 'Hokkaido'
    },
    'tohoku': {
        'name': 'Tohoku Electric Power Company',
        'area_code': '02',
        'region': 'Tohoku'
    },
    'tepco': {
        'name': 'Tokyo Electric Power Company',
        'area_code': '03',
        'region': 'Tokyo'
    },
    'chubu': {
        'name': 'Chubu Electric Power Company',
        'area_code': '04',
        'region': 'Chubu'
    },
    'hokuriku': {
        'name': 'Hokuriku Electric Power Company',
        'area_code': '05',
        'region': 'Hokuriku'
    },
    'kepco': {
        'name': 'Kansai Electric Power Company',
        'area_code': '06', 
        'region': 'Kansai'
    },
    'chugoku': {
        'name': 'Chugoku Electric Power Company',
        'area_code': '07',
        'region': 'Chugoku'
    },
    'shikoku': {
        'name': 'Shikoku Electric Power Company',
        'area_code': '08',
        'region': 'Shikoku'
    },
    'kyushu': {
        'name': 'Kyushu Electric Power Company',
        'area_code': '09',
        'region': 'Kyushu'
    }
}

def get_tso_url(tso_id: str, url_type: str = 'demand') -> Optional[str]:
    """
    指定されたTSOとURLタイプに対応するURLを取得
    
    Args:
        tso_id: TSO ID（例: 'tepco'）
        url_type: URLタイプ（'demand'または'supply'）
        
    Returns:
        対応するURL文字列（見つからない場合はNone）
    """
    if tso_id not in VALID_TSO_IDS:
        return None
        
    try:
        # 設定ファイルから読み込み
        config_path = os.path.join(project_root, 'config', 'tso_urls.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 対応するURLを返す
        if tso_id in config and url_type in config[tso_id]:
            return config[tso_id][url_type]
            
        return None
    except Exception as e:
        print(f"Error loading TSO URL configuration: {e}")
        return None

def get_area_code(tso_id: str) -> Optional[str]:
    """
    TSO IDに対応するエリアコードを取得
    
    Args:
        tso_id: TSO ID（例: 'tepco'）
        
    Returns:
        対応するエリアコード（見つからない場合はNone）
    """
    if tso_id in TSO_INFO:
        return TSO_INFO[tso_id]['area_code']
    return None

def get_tso_by_area_code(area_code: str) -> Optional[str]:
    """
    エリアコードに対応するTSO IDを取得
    
    Args:
        area_code: エリアコード（例: '03'）
        
    Returns:
        対応するTSO ID（見つからない場合はNone）
    """
    for tso_id, info in TSO_INFO.items():
        if info['area_code'] == area_code:
            return tso_id
    return None

def get_tso_name(tso_id: str) -> Optional[str]:
    """
    TSO IDに対応する電力会社名を取得
    
    Args:
        tso_id: TSO ID（例: 'tepco'）
        
    Returns:
        対応する電力会社名（見つからない場合はNone）
    """
    if tso_id in TSO_INFO:
        return TSO_INFO[tso_id]['name']
    return None 