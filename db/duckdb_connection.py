# db/duckdb_connection.py
import os
import atexit
from pathlib import Path
from typing import Optional, Tuple, Union

import duckdb
import pandas as pd
import numpy as np
import tempfile
import time


class DuckDBConnection:
    """
    Context‑manager friendly wrapper around duckdb.connect.

    * 既定では書き込み可能な接続を取得します。
    * with 文で囲むと確実に close され、DuckDB のロックが残りません。
    * read‑only 接続を取りたい場合は `read_only=True` を渡してください。
    """

    def __init__(
        self,
        db_path: Optional[Union[str, Path]] = None,
        *,
        read_only: bool = False,
    ) -> None:
        self.db_path = (
            str(db_path)
            if db_path is not None
            else os.getenv(
                "DB_PATH",
                "/Volumes/MacMiniSSD/powermarketdata/power_market_data",
            )
        )
        # フォルダが無い場合は作成
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)

        self.read_only = read_only
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

        # atexit で確実にクローズ
        atexit.register(self.close)

    # ------------------------------------------------------------------ #
    # Context‑manager support
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "DuckDBConnection":
        if self._connection is None:
            self._connection = duckdb.connect(
                self.db_path,
                read_only=self.read_only,
            )
            print(f"[INFO] Opened DuckDB connection: {self.db_path} (read_only={self.read_only})")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Public helpers
    # ------------------------------------------------------------------ #
    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        commit: bool = True,
    ) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query and return its relation.

        Parameters
        ----------
        query : str
            SQL statement.
        params : tuple, optional
            Parameters for parametrised query.
        commit : bool
            Whether to call commit() after executing. Ignored on read‑only
            connections.
        """
        self._ensure_connection()

        try:
            print(
                f"[DEBUG] DuckDB ({'RO' if self.read_only else 'RW'}) {self.db_path} : "
                f"{query[:120]}"
            )
            if params is not None:
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            if commit and not self.read_only:
                self._connection.commit()

            return result
        except Exception as e:
            print(f"[ERROR] Error executing query: {e}")
            raise  # re‑raise so caller can see the stack

    def close(self) -> None:
        """Close the underlying DuckDB connection (if it exists)."""
        if self._connection is not None:
            try:
                self._connection.close()
                print(f"[INFO] Closed DuckDB connection: {self.db_path}")
            except Exception as e:
                print(f"[WARN] Failed to close DuckDB connection: {e}")
            finally:
                self._connection = None
    
    def is_connected(self) -> bool:
        """Check if database connection is currently open."""
        return self._connection is not None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _ensure_connection(self) -> None:
        """Open a connection lazily if it doesn't exist yet."""
        if self._connection is None:
            self._connection = duckdb.connect(
                self.db_path,
                read_only=self.read_only,
            )
            print(f"[INFO] Opened DuckDB connection: {self.db_path} (read_only={self.read_only})")
            
    def save_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'append',
        check_duplicate_master_key: bool = True
    ) -> int:
        """
        DataFrameをDuckDBのテーブルに保存する。
        'append' 動作のみサポート。'replace' や 'fail' は未対応。
        ★ master_key による重複チェック機能を追加。
        
        Args:
            df: 保存するDataFrame
            table_name: 保存先テーブル名
            if_exists: テーブルが存在する場合の動作 (現在は 'append' のみ有効)
            check_duplicate_master_key: Trueの場合、master_keyで重複チェックを行う
            
        Returns:
            int: 実際に挿入された行数
            
        Raises:
            ValueError: if_exists が 'append' 以外の場合や、必須カラムがない場合
            Exception: データベース操作エラー
        """
        inserted_rows = 0
        if if_exists != 'append':
            raise ValueError("save_dataframe currently only supports if_exists='append'")

        if df.empty:
            print(f"[WARNING] 保存対象のDataFrameが空です。Table={table_name}")
        return 0
            
        # ★ master_key の存在チェック (重複チェックする場合)
        if check_duplicate_master_key and 'master_key' not in df.columns:
            raise ValueError(f"重複チェックが有効ですが、DataFrameに 'master_key' カラムが存在しません。Table={table_name}")
        
        # NULLチェック (master_keyのみ、重複チェックする場合)
        if check_duplicate_master_key and df['master_key'].isna().any():
            null_count = df['master_key'].isna().sum()
            print(f"[WARNING] 重複チェックキー 'master_key' がNULLの行が{null_count}件あります。これらの行は除外して処理します。")
            df = df[df['master_key'].notna()].reset_index(drop=True)
            if df.empty:
                print(f"[WARNING] NULL master_key 除外後のDataFrameが空になりました。Table={table_name}")
                return 0

        # 一時ビュー名（一意にする）
        temp_view_name = f"temp_view_{table_name}_{int(time.time() * 1000)}"
        target_table = table_name

        try:
            self._ensure_connection()

            # テーブルの存在とスキーマ確認 (初回または必要に応じて)
            # TODO: テーブルスキーマがDataFrameと一致するか確認するロジックを追加するとより堅牢になる
            try:
                # テーブルが存在するか軽くチェック
                self._connection.execute(f"SELECT * FROM {target_table} LIMIT 1")
                table_exists = True
            except duckdb.CatalogException:
                 print(f"[WARNING] テーブル '{target_table}' が存在しません。スキーマから作成される想定です。")
                 table_exists = False
                 # 本来はここで CREATE TABLE するべきだが、
                 # TSODataImporter._ensure_tables() で作成されている前提とする
                 # もし存在しない場合、後続の INSERT でエラーになる
            except Exception as e:
                print(f"[ERROR] テーブル '{target_table}' の存在確認中にエラー: {e}")
                raise # 予期せぬエラーは上位に投げる
            
            # DataFrameを一時ビューとして登録
            self._connection.register(temp_view_name, df)
            print(f"[INFO] 一時ビュー '{temp_view_name}' を登録しました ({len(df)}行)。")

            # 挿入クエリの構築
            if check_duplicate_master_key:
                # 重複チェックを行う場合: master_key がターゲットテーブルに存在しない行のみ挿入
                insert_query = f"""
                INSERT INTO {target_table}
                SELECT * FROM {temp_view_name} tmp
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_table} existing
                    WHERE existing.master_key = tmp.master_key
                );
                """
                print(f"[INFO] 重複チェック付きINSERTクエリ実行: {target_table} <- {temp_view_name}")
            else:
                # 重複チェックを行わない場合: 全件挿入
                insert_query = f"INSERT INTO {target_table} SELECT * FROM {temp_view_name};"
                print(f"[INFO] 全件INSERTクエリ実行: {target_table} <- {temp_view_name}")

            # クエリ実行と挿入行数の取得
            try:
                # DuckDB の execute は Relation オブジェクトを返す。
                # INSERT文の実行で挿入された行数を直接取得する標準的な方法が見当たらない場合がある。
                # 回避策として、事前にターゲットテーブルの行数を数えておき、実行後の行数との差分を見る方法もあるが、
                # パフォーマンス影響や同時実行の問題があるため、ここでは execute を実行し、
                # 影響を受けた行数を返す cursor().rowcount のような機能がないか確認 (DuckDBPyRelationにはなさそう)
                # 代わりに、INSERT後のメッセージ等から判断するか、クエリ自体を工夫する必要があるかもしれない。
                # ここでは、execute() を呼び出し、エラーがなければ成功したとみなし、
                # 挿入を試みた行数から重複を除いた数を推定する (ただし正確ではない)。
                # →より確実な方法: INSERT ... RETURNING * を使うか、行数を別途カウントする

                # 方法A: 素直に実行 (行数は取れないかもしれない)
                # self.execute_query(insert_query, commit=True) # commitは不要かも
                # inserted_rows = -1 # 不明を示す値

                # 方法B: CTEで行数をカウントしてから挿入 (少し複雑)
                # with self._connection.cursor() as cursor:
                #    cursor.execute(insert_query)
                #    inserted_rows = cursor.rowcount # これは標準DB-APIだがDuckDBで使えるか？

                # 方法C: INSERT ... RETURNING を使う (master_keyだけでも返せば数がわかる)
                # DuckDBがINSERT ... RETURNINGをサポートしているか確認が必要。
                # (バージョン0.7.0以降でサポートされている模様)
                returning_query = insert_query.strip().rstrip(';') + " RETURNING master_key;"
                try:
                    result_rel = self.execute_query(returning_query)
                    returned_keys = result_rel.fetchall()
                    inserted_rows = len(returned_keys) if returned_keys is not None else 0
                    print(f"[INFO] INSERT...RETURNING 成功。実際に挿入された行数: {inserted_rows}")
                except Exception as return_err:
                    print(f"[WARNING] INSERT...RETURNING が失敗しました ({return_err})。通常のINSERTを試みます。挿入行数は推定になります。")
                    # RETURNINGなしで再実行
                    self.execute_query(insert_query)
                    # この場合、正確な挿入行数は不明。重複がなければ df の行数になるはず。
                    # 重複チェックした場合の推定挿入行数 (非常に不正確になる可能性あり)
                    if check_duplicate_master_key:
                         # この推定はDBの状態に依存するため、ここで行うのは難しい
                         inserted_rows = -1 # 不明を示す
                         print(f"[WARNING] 通常のINSERTを実行しましたが、正確な挿入行数は取得できませんでした。")
                    else:
                         inserted_rows = len(df)
                         print(f"[INFO] 通常のINSERTを実行。全件({inserted_rows}行)挿入を試みました。")

            except Exception as e:
                print(f"[ERROR] データ挿入エラー ({target_table} <- {temp_view_name}): {e}")
                # エラー詳細 (例: スキーマ不一致など) をデバッグ用に表示
                try:
                    target_schema = self._connection.execute(f"PRAGMA table_info({target_table});").fetchall()
                    temp_schema = self._connection.execute(f"PRAGMA table_info({temp_view_name});").fetchall()
                    print(f"[DEBUG] Target Table Schema ({target_table}): {target_schema}")
                    print(f"[DEBUG] Temp View Schema ({temp_view_name}): {temp_schema}")
                except Exception as ie:
                    print(f"[ERROR] スキーマ情報の取得中にエラー: {ie}")
                raise # 元のエラーを再送出

        finally:
            # 一時ビューの登録解除
            try:
                if self._connection:
                    self._connection.unregister(temp_view_name)
                    print(f"[INFO] 一時ビュー '{temp_view_name}' の登録を解除しました。")
            except Exception as unreg_err:
                # すでに接続が閉じられている場合など
                print(f"[WARNING] 一時ビュー '{temp_view_name}' の登録解除エラー: {unreg_err}")
        
        # 不明(-1) でない場合のみログ出力
        if inserted_rows != -1:
            print(f"[INFO] save_dataframe 完了: Table='{target_table}', Inserted={inserted_rows} rows")
        else:
             print(f"[WARNING] save_dataframe 完了: Table='{target_table}', 挿入行数不明")
            
        return inserted_rows if inserted_rows != -1 else 0 # 不明時は0を返す

# Example usage of the context manager
if __name__ == "__main__":
    # Example 1: Using with statement for write operations
    with DuckDBConnection() as db:
        db.execute_query("INSERT INTO my_table (column1, column2) VALUES (?, ?)", (123, "test"))
        print("Data inserted successfully")
    # Connection is automatically closed when the with block exits
    
    # Example 2: Using with statement for read-only operations
    with DuckDBConnection(read_only=True) as db:
        result = db.execute_query("SELECT * FROM my_table LIMIT 10")
        for row in result.fetchall():
            print(row)
    # Connection is automatically closed when the with block exits