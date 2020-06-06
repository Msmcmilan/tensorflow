#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
IG Markets REST API sample with Python
2015 FemtoTrader
"""

from ig_stream.trading_ig import IGService
from ig_stream.trading_ig.config import config
import logging

from rad_sa_bazom import dataframe_u_bazu

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# if you need to cache to DB your requests
from datetime import timedelta
import requests_cache
import datetime

def main():
    logging.basicConfig(level=logging.DEBUG)

    expire_after = timedelta(hours=1)
    session = requests_cache.CachedSession(
        cache_name='cache', backend='sqlite', expire_after=expire_after
    )
    # set expire_after=None if you don't want cache expiration
    # set expire_after=0 if you don't want to cache queries

    #config = IGServiceConfig()

    # no cache
    ig_service = IGService(
        config.username, config.password, config.api_key, config.acc_type
    )

    # if you want to globally cache queries
    #ig_service = IGService(config.username, config.password, config.api_key, config.acc_type, session)

    ig_service.create_session()

  #  accounts = ig_service.fetch_accounts()
  #  print("accounts:\n%s" % accounts)

    #account_info = ig_service.switch_account(config.acc_number, False)
    # print(account_info)

    #open_positions = ig_service.fetch_open_positions()
    #print("open_positions:\n%s" % open_positions)

    print("")

    #working_orders = ig_service.fetch_working_orders()
    #print("working_orders:\n%s" % working_orders)

    print("")

    #epic = 'CS.D.EURUSD.MINI.IP'
    #epic = 'IX.D.ASX.IFM.IP'  # US (SPY) - mini
      # US (SPY) - mini

    #resolution = 'D'

    # see from pandas.tseries.frequencies import to_offset
    #resolution = 'H'
    #resolution = '1Min'

 #   num_points = 9900
#    response = ig_service.fetch_historical_prices_by_epic_and_num_points(
#        epic, resolution, num_points
#    )
    #CS.D.GBPUSD.TODAY.IP   CS.D.EURUSD.TODAY.IP   CS.D.USDJPY.TODAY.IP
  #  epici = {'DOW': 'IX.D.DOW.DAILY.IP','DAX': 'IX.D.DAX.DAILY.IP','FTSE': 'IX.D.FTSE.DAILY.IP','GBPUSD': 'CS.D.GBPUSD.TODAY.IP','EURUSD': 'CS.D.EURUSD.TODAY.IP','USDJPY': 'CS.D.USDJPY.TODAY.IP'}
    #epici = {'GBPUSD': 'CS.D.GBPUSD.TODAY.IP','EURUSD': 'CS.D.EURUSD.TODAY.IP','USDJPY': 'CS.D.USDJPY.TODAY.IP'}
    epici = {'DAX': 'IX.D.DOW.DAILY.IP'}
    #treba foreks i DAX
    resolution = '1Min'
    start_date = '2020-06-03 00:53:00'
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date = '2020-06-06 13:18:01'
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

    for key in epici:
        epic = epici[key]
        sifra = key
        print("Ucitavamo: " + sifra)
        response = ig_service.fetch_historical_prices_by_epic_and_date_range(
            epic, resolution, start_date, end_date)
        try:
            df_za_bazu = ((response['prices']['ask']['Open'] + response['prices']['bid']['Open']) / 2).to_frame()
            df_za_bazu['hi'] = (response['prices']['ask']['High'] + response['prices']['bid']['High']) / 2
            df_za_bazu['lo'] = (response['prices']['ask']['Low'] + response['prices']['bid']['Low']) / 2
            df_za_bazu['cl'] = (response['prices']['ask']['Close'] + response['prices']['bid']['Close']) / 2
            try:
                df_za_bazu['vol'] = response['prices']['last']['Volume']
            except BaseException:
                print("Greska kod vol:" + sifra)
            df_za_bazu['datum'] = response['prices']['ask']['Open']._index
            # df_za_bazu['open'] = ((response['prices']['ask']['Open'] + response['prices']['bid']['Open'])/2).to_frame()
            df_za_bazu = df_za_bazu.rename(columns={"Open": "op"})

            dataframe_u_bazu.insert_u_presek(df_za_bazu, sifra, "PRESEK_MINUT")
        except BaseException:
            print("Greska kod:" + sifra)
    #import pandas as pd
    #columns = ['datum', 'open']
    #df_za_bazu = pd.DataFrame(columns=columns)

    #df_za_bazu['datum'] = response['prices']['ask']['Open']._index

    #print(df_za_bazu)
    # Exception: error.public-api.exceeded-account-historical-data-allowance

    # if you want to cache this query
    #response = ig_service.fetch_historical_prices_by_epic_and_num_points(epic, resolution, num_points, session)

    #df_ask = response['prices']['ask']
    #response['prices']['ask']['Open']._index[1]
    #print("ask prices:\n%s" % df_ask)

    #(start_date, end_date) = ('2015-09-15', '2015-09-28')
    #response = ig_service.fetch_historical_prices_by_epic_and_date_range(epic, resolution, start_date, end_date)

    # if you want to cache this query
    #response = ig_service.fetch_historical_prices_by_epic_and_date_range(epic, resolution, start_date, end_date, session)
    #df_ask = response['prices']['ask']
    #print("ask prices:\n%s" % df_ask)


if __name__ == '__main__':
    main()
