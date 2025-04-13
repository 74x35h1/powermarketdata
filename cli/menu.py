#!/usr/bin/env python3
import sys
import time
from typing import Dict, Callable

class Menu:
    def __init__(self):
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

    def _download_jepx_price(self):
        print("[Starting retrieval of JEPX Price Data...]")
        # TODO: Implement JEPX Price Data retrieval

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

    def _download_tso_demand(self):
        print("[Starting retrieval of Supply and Demand Data from TSO...]")
        # TODO: Implement TSO supply and demand data retrieval

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