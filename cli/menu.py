#!/usr/bin/env python3
import sys
import time
import os
import subprocess # Import subprocess module
from typing import Dict, Callable, Optional
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
    from db.duckdb_connection import DuckDBConnection
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

    def _get_date_input(self, prompt: str, default_date: date) -> Optional[date]:
        """Prompts the user for a date in YYYY-MM-DD format and validates it."""
        while True:
            try:
                date_str = input(f"{prompt} (YYYY-MM-DD) [default: {default_date.strftime('%Y-%m-%d')}]: ").strip()
                if not date_str:
                    return default_date
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD.")
            except Exception as e:
                 print(f"An error occurred: {e}")
                 return None # Indicate error

    def _display_tso_choices(self):
        """TSO選択用の番号付きリストを表示します"""
        tso_ids = {
            "hokkaido": {"name": "Hokkaido Electric Power Network", "area_code": "1"},
            "tohoku": {"name": "Tohoku Electric Power Network", "area_code": "2"},
            "tepco": {"name": "TEPCO Power Grid", "area_code": "3"},
            "chubu": {"name": "Chubu Electric Power Grid", "area_code": "4"},
            "hokuriku": {"name": "Hokuriku Electric Power Company", "area_code": "5"},
            "kansai": {"name": "Kansai Electric Power", "area_code": "6"},
            "chugoku": {"name": "Chugoku Electric Power", "area_code": "7"},
            "shikoku": {"name": "Shikoku Electric Power Company", "area_code": "8"},
            "kyushu": {"name": "Kyushu Electric Power", "area_code": "9"}
        }
        
        print("\nTSO Area Selection:")
        print("-" * 60)
        print(f"{'No.':<4} {'Area Code':<10} {'TSO Name':<30}")
        print("-" * 60)
        
        tso_choice_map = {}
        
        # 番号付きでTSOリストを表示
        for i, (tso_id, info) in enumerate(sorted(tso_ids.items(), key=lambda x: x[1]['area_code']), 1):
            print(f"{i:<4} {info['area_code']:<10} {info['name']:<30}")
            tso_choice_map[str(i)] = tso_id
        
        print("-" * 60)
        
        return tso_choice_map
    
    def _get_tso_selection(self, tso_choice_map):
        """ユーザーからTSO選択を取得します"""
        while True:
            choice = input("Select area (enter number): ").strip()
            
            if choice in tso_choice_map:
                return [tso_choice_map[choice]]  # 選択されたTSO
            else:
                print(f"Invalid selection. Please enter a number between 1-{len(tso_choice_map)}")
    
    def _download_tso_demand(self):
        print("[Starting retrieval of Supply and Demand Data from TSO...]")
        # 月範囲を取得
        start_date, end_date = self._get_month_range()
        
        # 番号付きのTSO選択リストを表示
        tso_choice_map = self._display_tso_choices()
        tso_ids = self._get_tso_selection(tso_choice_map)
        
        try:
            print(f"\nDownloading TSO demand data for {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')}...")
            print(f"Selected areas: {', '.join(tso_ids)}")
            
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
        print("\n[Starting retrieval of JEPX Bid Data...]")
        try:
            # self.portal は main.PowerMarketPortal のインスタンス
            self.portal.download_jepx_bid_data() # ★ PowerMarketPortalのメソッドを呼び出す
        except Exception as e:
            print(f"Error during JEPX Bid Data retrieval: {str(e)}")
            logger.error(f"Error in _download_jepx_bid: {e}", exc_info=True)

    def _download_hjks(self):
        print("[Starting retrieval of HJKS Data...]")
        # TODO: Implement HJKS Data retrieval

    def _download_occto_plant(self):
        print("\n[Starting OCCTO 30-min Generation Data Download and DB Import...]")
        
        # Get start and end dates from user
        today = date.today()
        default_start_date = today - timedelta(days=7)
        default_end_date = today - timedelta(days=1)

        start_date = self._get_date_input("Enter start date", default_start_date)
        if start_date is None: return # Error occurred
        
        end_date = self._get_date_input("Enter end date", default_end_date)
        if end_date is None: return # Error occurred

        if end_date < start_date:
            print("End date cannot be earlier than start date. Aborting.")
            return

        # Prepare the command to run the script as a module
        command = [
            sys.executable, # Use the current Python interpreter
            "-m", "data_sources.occto.30min_gendata_downloader", # Run as a module
            "--start-date", start_date.strftime('%Y-%m-%d'),
            "--end-date", end_date.strftime('%Y-%m-%d')
            # Add --db-path if you want to specify it here, otherwise it uses the default
            # "--db-path", "path/to/your.db"
        ]

        print(f"\nExecuting command: {' '.join(command)}")

        try:
            # Run the script as a subprocess, ensuring the CWD is the project root
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                check=True,
                cwd=project_root # Set the current working directory to the project root
            )
            print("--- Script Output Start ---")
            print(result.stdout)
            if result.stderr:
                 print("--- Script Error Output Start ---")
                 print(result.stderr)
                 print("--- Script Error Output End ---")
            print("--- Script Output End ---")
            print("\n[OCCTO 30-min data download and import process completed successfully.]")

        except FileNotFoundError:
            print(f"[ERROR] Python interpreter not found or script path incorrect: {sys.executable}")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Script execution failed with exit code {e.returncode}.")
            print("--- Script Output Start ---")
            print(e.stdout)
            print("--- Script Error Output Start ---")
            print(e.stderr)
            print("--- Script Error Output End ---")
        except Exception as e:
            print(f"An unexpected error occurred while running the script: {e}")

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