#!/usr/bin/env python3
import sys
import time
import os
from typing import Dict, Callable
from datetime import date, datetime, timedelta
from calendar import monthrange

# パスの設定
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ロギング設定
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# メインポータルモジュールをインポート
try:
    from data_sources.tso.unified_downloader import UnifiedTSODownloader
    from data_sources.tso.db_importer import TSODataImporter
    from data_sources.db_connection import DuckDBConnection
except ImportError as e:
    logger.error(f"モジュールインポートエラー: {e}")
    logger.error("必要なモジュールがインポートできません。")
    sys.exit(1)

class Menu:
    def __init__(self):
        # 遅延importで循環参照を回避
        from main import PowerMarketPortal
        self.portal = PowerMarketPortal()
        
        self.menu_options: Dict[str, Callable] = {
            "1": self._download_jepx_price,
            "2": self._download_jepx_bid,
            "3": self._download_hjks,
            "4": self._download_occto_plant,
            "5": self._download_occto_interconnection,
            "6": self._download_occto_reserve,
            "7": self._download_eprx,
            "8": self._download_tso_demand,
            "9": self._download_jma_weather,
            "10": self._download_eex_futures,
            "11": self._download_tocom_futures,
            "12": self._download_ice_jkm,
            "13": self._download_ice_ttf,
            "14": self._download_ice_nc_coal,
            "15": self._download_ice_dubai_crude,
            "16": self._download_ice_usdjpy,
            "q": self._exit_program
        }

    def display_menu(self):
        """Display the CLI menu in English."""
        print("\n===== Powermarketdata CLI =====")
        print("Please select an option by entering the corresponding number:")
        print("1.  JEPX Price Data")
        print("2.  JEPX Bid Data")
        print("3.  HJKS Data")
        print("4.  Power Plant Operation Data from OCCTO")
        print("5.  Interconnection Forecast Data from OCCTO")
        print("6.  Wide-area Reserve Data from OCCTO")
        print("7.  EPRX Price Data")
        print("8.  Supply and Demand Data from TSO")
        print("9.  Weather Data from JMA")
        print("10.  EEX Futures Price Data")
        print("11.  TOCOM Futures Price Data")
        print("12.  ICE JKM Futures Price Data")
        print("13.  ICE TTF Futures Price Data")
        print("14.  ICE NC Coal Price Data")
        print("15.  ICE Dubai Crude Oil Price Data")
        print("16.  ICE USD/JPY Futures Price Data")
        print("q. Quit")

    def _get_month_range(self):
        """ユーザーに月範囲を入力してもらう（YYYY-MM形式）"""
        today = date.today()
        default_year = today.year
        default_month = today.month
        try:
            start_input = input(f"Enter start month (YYYY-MM) [default: {default_year}-{default_month:02d}]: ").strip()
            if not start_input:
                start_year, start_month = default_year, default_month
            else:
                start_year, start_month = map(int, start_input.split('-'))
            end_input = input(f"Enter end month (YYYY-MM) [default: {default_year}-{default_month:02d}]: ").strip()
            if not end_input:
                end_year, end_month = default_year, default_month
            else:
                end_year, end_month = map(int, end_input.split('-'))
            # 月初・月末を自動計算
            start_date = date(start_year, start_month, 1)
            end_last_day = monthrange(end_year, end_month)[1]
            end_date = date(end_year, end_month, end_last_day)
            if end_date < start_date:
                print("End month must be after start month. Using default values.")
                return date(default_year, default_month, 1), date(default_year, default_month, monthrange(default_year, default_month)[1])
            return start_date, end_date
        except Exception:
            print("Invalid month format. Using default values.")
            return date(default_year, default_month, 1), date(default_year, default_month, monthrange(default_year, default_month)[1])

    def _download_tso_demand(self):
        print("[Starting retrieval of Supply and Demand Data from TSO...]")
        # 月範囲を取得
        start_date, end_date = self._get_month_range()
        # TSO IDを指定するか選択
        use_specific_tsos = input("Do you want to specify TSO IDs? (y/n) [n]: ").strip().lower() == 'y'
        tso_ids = None
        if use_specific_tsos:
            tso_input = input("Enter TSO IDs separated by space (e.g., tepco hokkaido): ").strip()
            if tso_input:
                tso_ids = tso_input.split()
        try:
            print(f"\nDownloading TSO demand data for {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')}...")
            demand_rows = self.portal.download_tso_data(
                start_date=start_date,
                end_date=end_date,
                tso_ids=tso_ids,
                url_type="demand"
            )
            print(f"Successfully imported {demand_rows} rows of TSO demand data.")
            print(f"Downloading TSO supply data for {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')}...")
            supply_rows = self.portal.download_tso_data(
                start_date=start_date,
                end_date=end_date,
                tso_ids=tso_ids,
                url_type="supply"
            )
            print(f"Successfully imported {supply_rows} rows of TSO supply data.")
            print(f"Total: {demand_rows + supply_rows} rows of TSO data imported.")
        except Exception as e:
            print(f"Error downloading TSO data: {str(e)}")

    def _download_jepx_price(self):
        print("[Starting retrieval of JEPX Price Data...]")
        try:
            rows = self.portal.download_jepx_price()
            print(f"Successfully imported {rows} rows of JEPX price data.")
        except Exception as e:
            print(f"Error importing JEPX price data: {str(e)}")

    def _download_jepx_bid(self):
        print("[Starting retrieval of JEPX Bid Data...]")
        # TODO: Implement JEPX Bid Data retrieval

    def _download_hjks(self):
        print("[Starting retrieval of HJKS Data...]")
        # TODO: Implement HJKS Data retrieval

    def _download_occto_plant(self):
        print("[Starting retrieval of Power Plant Operation Data from OCCTO...]")
        # TODO: Implement OCCTO power plant operation data retrieval

    def _download_occto_interconnection(self):
        print("[Starting retrieval of Interconnection Forecast Data from OCCTO...]")
        # TODO: Implement OCCTO interconnection forecast data retrieval

    def _download_occto_reserve(self):
        print("[Starting retrieval of Wide-area Reserve Data from OCCTO...]")
        # TODO: Implement OCCTO wide-area reserve data retrieval

    def _download_eprx(self):
        print("[Starting retrieval of EPRX Price Data...]")
        # TODO: Implement EPRX Price Data retrieval

    def _download_jma_weather(self):
        print("[Starting retrieval of Weather Data from JMA...]")
        # TODO: Implement JMA weather data retrieval

    def _download_eex_futures(self):
        print("[Starting retrieval of EEX Futures Price Data...]")
        # TODO: Implement EEX futures price data retrieval

    def _download_tocom_futures(self):
        print("[Starting retrieval of TOCOM Futures Price Data...]")
        # TODO: Implement TOCOM futures price data retrieval

    def _download_ice_jkm(self):
        print("[Starting retrieval of ICE JKM Futures Price Data...]")
        # TODO: Implement ICE JKM futures price data retrieval

    def _download_ice_ttf(self):
        print("[Starting retrieval of ICE TTF Futures Price Data...]")
        # TODO: Implement ICE TTF futures price data retrieval

    def _download_ice_nc_coal(self):
        print("[Starting retrieval of ICE NC Coal Price Data...]")
        # TODO: Implement ICE NC coal price data retrieval

    def _download_ice_dubai_crude(self):
        print("[Starting retrieval of ICE Dubai Crude Oil Price Data...]")
        # TODO: Implement ICE Dubai crude oil price data retrieval

    def _download_ice_usdjpy(self):
        print("[Starting retrieval of ICE USD/JPY Futures Price Data...]")
        # TODO: Implement ICE USD/JPY futures price data retrieval

    def _exit_program(self):
        print("Exiting program.")
        sys.exit(0)

    def run(self):
        """Main entry point for the Powermarketdata CLI."""
        while True:
            self.display_menu()
            selection = input("Your selection: ").strip().lower()
            
            if selection in self.menu_options:
                self.menu_options[selection]()
                # Pause briefly before showing the menu again
                time.sleep(1)
            else:
                print("Invalid selection. Please try again.")

if __name__ == "__main__":
    menu = Menu()
    menu.run()