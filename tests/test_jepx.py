#!/usr/bin/env python3
import requests
from datetime import datetime, timedelta
import sys
import time
import random
import argparse

# 定数設定
BASE_URL = "https://www.jepx.jp/js/csv_read.php"
DIR_NAMES = ["spot_bid_curves", "spot_splitting_areas"]

def download_csv(date, dir_name):
    """
    指定した日付とディレクトリ名に対するCSVをダウンロードし、その内容（バイナリ）を返す。
    """
    date_str = date.strftime("%Y%m%d")
    params = {
        "dir": dir_name,
        "file": f"{dir_name}_{date_str}.csv"
    }
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ja',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'www.jepx.jp',
        'If-Modified-Since': 'Thu, 01 Jun 1970 00:00:00 GMT',
        'Pragma': 'no-cache',
        'Referer': 'https://www.jepx.jp/electricpower/market-data/spot/bid_curves.html',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15'
    }
    try:
        with requests.Session() as session:
            response = session.get(BASE_URL, params=params, headers=headers)
            if response.status_code == 200:
                print(f"Downloaded: {dir_name}_{date_str}.csv")
                return response.content
            else:
                print(f"Failed to download {dir_name}_{date_str}.csv: Status code {response.status_code}")
                return None
    except Exception as e:
        print(f"Error downloading {dir_name}_{date_str}.csv: {e}")
        return None

def download_and_display(start_date, end_date):
    """
    指定した期間内の日付について、各カテゴリーのCSVをダウンロードし、その内容を表示する。
    """
    current_date = start_date
    while current_date <= end_date:
        for dir_name in DIR_NAMES:
            content = download_csv(current_date, dir_name)
            if content is not None:
                print(f"--- {dir_name}_{current_date.strftime('%Y%m%d')}.csv ---")
                try:
                    text = content.decode('utf-8')
                except UnicodeDecodeError:
                    text = content.decode('shift_jis', errors='replace')
                print(text)
                print()
        sleep_time = random.randint(10, 20)
        print(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)
        current_date += timedelta(days=1)

def interactive_input():
    """Prompt the user interactively for start and end dates."""
    start_date_str = input("Enter start date (YYYY-MM-DD): ")
    end_date_str = input("Enter end date (YYYY-MM-DD): ")
    return start_date_str, end_date_str

def main():
    start_date_str, end_date_str = interactive_input()

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    
    download_and_display(start_date, end_date)
    print("Download completed.")

if __name__ == "__main__":
    main()