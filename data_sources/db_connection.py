#!/usr/bin/env python3
"""
データベース接続モジュール

このモジュールはDuckDBデータベースへの接続とデータ操作を行うためのインターフェースを提供します。
初期化時にDBファイルのパスを指定することができ、指定がない場合はデフォルトパスが使用されます。
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple

# プロジェクトのルートディレクトリをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)

# 実際のデータベース実装をインポート
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    logger.warning("DuckDBがインストールされていません。モックデータベースが使用されます。")
    DUCKDB_AVAILABLE = False

class DuckDBConnection:
    """DuckDBデータベース接続を管理するクラス（インスタンスごとにdb_pathを持つ）"""
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.getenv("DB_PATH", "power_market_data.duckdb")
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self._connection = duckdb.connect(self.db_path)
        print(f"Connected to DuckDB at: {self.db_path}")

    def execute_query(self, query: str, params: tuple = None) -> Optional[Any]:
        """
        SQLクエリを実行
        
        Args:
            query: 実行するSQLクエリ
            params: クエリのパラメータ（オプション）
            
        Returns:
            クエリ結果
        """
        if not DUCKDB_AVAILABLE or self._connection is None:
            logger.info(f"モックDB: クエリを実行します: {query}")
            return None
        
        try:
            if params:
                return self._connection.execute(query, params)
            else:
                return self._connection.execute(query)
        except Exception as e:
            logger.error(f"クエリ実行エラー: {str(e)}")
            return None
    
    def save_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """
        DataFrameをデータベースに保存
        
        Args:
            df: 保存するDataFrame
            table_name: 保存先テーブル名
            
        Returns:
            保存された行数
        """
        if not DUCKDB_AVAILABLE or self._connection is None:
            logger.info(f"モックDB: {len(df)} 行を {table_name} テーブルに保存します")
            return len(df)
        
        try:
            self._connection.register("temp_df", df)
            self._connection.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
            return len(df)
        except Exception as e:
            logger.error(f"データフレーム保存エラー: {str(e)}")
            try:
                logger.info(f"テーブル {table_name} を作成します")
                self._connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df LIMIT 0")
                self._connection.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                return len(df)
            except Exception as inner_e:
                logger.error(f"テーブル作成とデータ保存エラー: {str(inner_e)}")
                return 0
    
    def get_connection(self):
        """
        DuckDB接続オブジェクトを取得
        
        Returns:
            DuckDB接続オブジェクト
        """
        return self._connection
    
    def close(self):
        """データベース接続を閉じる"""
        if DUCKDB_AVAILABLE and self._connection:
            self._connection.close()
            self._connection = None 