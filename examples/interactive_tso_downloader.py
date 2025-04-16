#!/usr/bin/env python
"""
電力会社（TSO）データダウンローダー - 対話形式CLI

このスクリプトは、対話形式のコマンドラインインターフェース（CLI）を提供し、
ユーザーが電力会社（TSO）とダウンロード月を選択できるようにします。
選択されたデータはダウンロードされ、コンソールに表示されます。
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

# 親ディレクトリをパスに追加してモジュールをインポートできるようにする
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.tso.unified_downloader import UnifiedTSODownloader
from data_sources.tso.tso_urls import TSO_INFO

# ロギングを設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_header():
    """アプリケーションのヘッダーを表示します。"""
    print("\n" + "=" * 60)
    print("  電力会社（TSO）データダウンローダー - 対話形式CLI  ".center(60))
    print("=" * 60)
    print("\n日本の電力会社から需要データをダウンロードして表示します。")
    print("対象のエリアコードと取得したい月を選択してください。\n")

def display_tso_choices() -> Dict[str, str]:
    """
    利用可能なTSOの選択肢を表示し、選択のためのマッピングを返します。
    
    Returns:
        選択肢のキーとTSO IDのマッピング
    """
    tso_choices = {}
    print("◆ 対象のエリアを選択してください:")
    
    for i, (tso_id, info) in enumerate(sorted(TSO_INFO.items(), key=lambda x: x[1]['area_code']), 1):
        tso_choices[str(i)] = tso_id
        print(f"  {i}. [{info['area_code']}] {info['name']} ({info['region']})")
    
    print(f"  A. すべてのエリア")
    tso_choices["A"] = "all"
    tso_choices["a"] = "all"
    
    return tso_choices

def get_tso_selection(tso_choices: Dict[str, str]) -> List[str]:
    """
    ユーザーにTSOの選択を求めます。
    
    Args:
        tso_choices: 選択肢のキーとTSO IDのマッピング
    
    Returns:
        選択されたTSO IDのリスト
    """
    while True:
        selection = input("\n選択してください (番号またはA): ").strip()
        
        if selection in tso_choices:
            selected_tso = tso_choices[selection]
            if selected_tso == "all":
                print("\n✓ すべてのエリアを選択しました。")
                return list(TSO_INFO.keys())
            else:
                info = TSO_INFO[selected_tso]
                print(f"\n✓ 選択されたエリア: [{info['area_code']}] {info['name']} ({info['region']})")
                return [selected_tso]
        else:
            print("❌ 無効な選択です。番号またはAを入力してください。")

def get_date_selection() -> Tuple[int, int]:
    """
    ユーザーに年月の選択を求めます。
    
    Returns:
        選択された年と月のタプル (year, month)
    """
    current_date = datetime.now()
    
    # 年の選択
    while True:
        year_input = input(f"\n◆ 年を選択してください [{current_date.year}]: ").strip()
        
        if not year_input:
            year = current_date.year
            break
            
        try:
            year = int(year_input)
            if 2010 <= year <= current_date.year:
                break
            else:
                print(f"❌ 年は2010から{current_date.year}の間で入力してください。")
        except ValueError:
            print("❌ 有効な年を入力してください。")
    
    # 月の選択
    max_month = current_date.month if year == current_date.year else 12
    
    while True:
        month_input = input(f"\n◆ 月を選択してください [1-{max_month}]: ").strip()
        
        try:
            month = int(month_input)
            if 1 <= month <= max_month:
                break
            else:
                print(f"❌ 月は1から{max_month}の間で入力してください。")
        except ValueError:
            print("❌ 有効な月を入力してください。")
    
    print(f"\n✓ 選択された期間: {year}年{month}月")
    return year, month

def download_and_display_data(tso_ids: List[str], year: int, month: int) -> None:
    """
    指定されたTSOと年月のデータをダウンロードして表示します。
    
    Args:
        tso_ids: ダウンロードするTSO IDのリスト
        year: 対象年
        month: 対象月
    """
    print("\n" + "=" * 60)
    print("  データのダウンロードを開始します  ".center(60))
    print("=" * 60 + "\n")
    
    # ダウンローダーを初期化（DB接続なし）
    downloader = UnifiedTSODownloader(
        tso_ids=tso_ids,
        db_connection=None,  # DB保存は実装しない
        url_type='demand'
    )
    
    # 対象月の初日
    target_date = date(year, month, 1)
    
    all_results = []
    
    for tso_id in tso_ids:
        tso_info = TSO_INFO[tso_id]
        print(f"\n◆ [{tso_info['area_code']}] {tso_info['name']}のデータをダウンロード中...")
        
        try:
            # CSVデータをダウンロード
            csv_content = downloader.download_csv(target_date, tso_id)
            
            # CSVを処理
            df = downloader.process_csv(csv_content, target_date, tso_id)
            
            if df is not None and not df.empty:
                print(f"✓ データを正常に取得しました（{len(df)}行）")
                
                # 結果をリストに追加
                all_results.append((tso_id, df))
            else:
                print("❌ データが空または取得できませんでした")
                
        except Exception as e:
            print(f"❌ エラー: {str(e)}")
    
    # 結果の表示
    if all_results:
        display_results(all_results)
    else:
        print("\n❌ データを取得できませんでした。別の期間を試してください。")

def display_results(results: List[Tuple[str, pd.DataFrame]]) -> None:
    """
    ダウンロードしたデータを表示します。
    
    Args:
        results: (tso_id, dataframe)のタプルのリスト
    """
    print("\n" + "=" * 60)
    print("  ダウンロードしたデータ  ".center(60))
    print("=" * 60 + "\n")
    
    for tso_id, df in results:
        tso_info = TSO_INFO[tso_id]
        print(f"\n◆ [{tso_info['area_code']}] {tso_info['name']}")
        print("-" * 60)
        
        # 表示する列を選択
        display_columns = []
        
        # すべての利用可能な列を確認
        for col in ['date', 'hour', 'time_slot', 'demand_actual', 'demand_forecast']:
            if col in df.columns:
                display_columns.append(col)
        
        # データフレームを表示
        if display_columns:
            print(df[display_columns].head(10).to_string(index=False))
            
            if len(df) > 10:
                print(f"... 他 {len(df) - 10} 行")
        else:
            print(df.head(10).to_string(index=False))
            
            if len(df) > 10:
                print(f"... 他 {len(df) - 10} 行")
        
        print("-" * 60)

def main():
    """メイン関数"""
    try:
        print_header()
        
        # TSO選択
        tso_choices = display_tso_choices()
        selected_tsos = get_tso_selection(tso_choices)
        
        # 日付選択
        year, month = get_date_selection()
        
        # データのダウンロードと表示
        download_and_display_data(selected_tsos, year, month)
        
        print("\n処理が完了しました。")
        
    except KeyboardInterrupt:
        print("\n\nプログラムが中断されました。")
        return 1
    except Exception as e:
        print(f"\n\n予期せぬエラーが発生しました: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 