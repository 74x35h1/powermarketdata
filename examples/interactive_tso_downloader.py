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
from data_sources.tso.tso_urls import TSO_INFO, get_tso_id_from_area_code

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

def print_tso_choices():
    """Print available TSO choices to the user."""
    print("\nAvailable Area Codes:")
    print("-" * 60)
    print(f"{'Code':<6} {'TSO Name':<40} {'Region':<10}")
    print("-" * 60)
    
    # Sort by area code for consistent display
    sorted_tsos = sorted(TSO_INFO.items(), key=lambda x: x[1]['area_code'])
    
    for tso_id, info in sorted_tsos:
        print(f"{info['area_code']:<6} {info['name']:<40} {info['region']:<10}")
    
    print("-" * 60)

def get_area_code():
    """Get area code selection from the user."""
    while True:
        # Get all available area codes
        area_codes = sorted([info['area_code'] for info in TSO_INFO.values()])
        
        print("\nPlease enter the area code (e.g., 01, 02, 03...):")
        area_code = input("Area code: ").strip()
        
        if area_code in area_codes:
            return area_code
        else:
            print(f"Invalid area code. Please choose from: {', '.join(area_codes)}")

def get_year_and_month():
    """Get year and month from user input."""
    today = datetime.now()
    
    while True:
        try:
            year_input = input(f"\nEnter year (YYYY) [default: {today.year}]: ").strip()
            year = int(year_input) if year_input else today.year
            
            if year < 2010 or year > today.year + 1:
                print(f"Please enter a year between 2010 and {today.year + 1}")
                continue
            
            month_input = input(f"Enter month (1-12) [default: {today.month}]: ").strip()
            month = int(month_input) if month_input else today.month
            
            if month < 1 or month > 12:
                print("Please enter a month between 1 and 12")
                continue
            
            # Check if the date is in the future
            target_date = date(year, month, 1)
            today_date = date(today.year, today.month, 1)
            
            if target_date > today_date:
                confirm = input("The selected date is in the future. Continue? (y/n): ").lower()
                if confirm != 'y':
                    continue
            
            return year, month
        except ValueError:
            print("Please enter valid numbers for year and month")

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

def download_and_display_data(area_code, year, month):
    """
    Download data for the specified area code and date.
    
    Args:
        area_code (str): The area code to download data for
        year (int): The year to download data for
        month (int): The month to download data for
        
    Returns:
        pd.DataFrame or None: The downloaded data or None if download failed
    """
    try:
        # Get TSO ID from area code
        tso_id = get_tso_id_from_area_code(area_code)
        if not tso_id:
            print(f"No TSO found for area code {area_code}")
            return None
        
        # Create target date
        target_date = pd.Timestamp(year=year, month=month, day=1)
        
        # Create downloader
        downloader = UnifiedTSODownloader(tso_id=tso_id)
        
        # Download data
        data = downloader.download_for_month(target_date)
        
        if data is None or data.empty:
            print(f"No data available for {tso_id} in {year}-{month:02d}")
            return None
            
        return data
        
    except Exception as e:
        logging.error(f"Error downloading data: {str(e)}", exc_info=True)
        print(f"Error downloading data: {str(e)}")
        return None

def display_results(df):
    """
    Display the downloaded data.
    
    Args:
        df (pd.DataFrame): The data to display
    """
    # Display basic info
    print(f"\nDownloaded data contains {len(df)} rows")
    
    # Show data summary
    print("\nData summary:")
    print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
    
    # Display the first few rows
    print("\nSample data:")
    print(df.head().to_string())
    
    # Ask if user wants to see more
    if input("\nShow full dataset? (y/n): ").lower() == 'y':
        pd.set_option('display.max_rows', None)
        print(df.to_string())
        pd.reset_option('display.max_rows')

def main():
    """Run the interactive TSO data downloader CLI application."""
    try:
        # Print application header
        print_header()
        
        # Display TSO choices
        print_tso_choices()
        
        # Get area code from user
        area_code = get_area_code()
        
        # Get year and month
        year, month = get_year_and_month()
        
        # Download and display data
        print(f"\nDownloading data for area code {area_code} for {year}-{month:02d}...")
        df = download_and_display_data(area_code, year, month)
        
        # Display results if data was downloaded
        if df is not None:
            display_results(df)
            
        print("\nThank you for using the TSO Data Downloader!")
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main() 