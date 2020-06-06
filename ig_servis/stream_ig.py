#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
IG Markets Stream API sample with Python
2015 FemtoTrader
"""

import time
import sys
import traceback
import logging

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from ig_stream.trading_ig.rest import IGService
from ig_stream.trading_ig.stream import IGStreamService
from ig_stream.trading_ig.config import config
from ig_stream.trading_ig.lightstreamer import Subscription

import pandas as pd
columns = ['datum','sifra','op','cl','hi','lo','cl','vol']
df_presek_minut = pd.DataFrame(columns=columns)
columns = ['datum','datum_unosa','sifra','bid','ask']
df_tik = pd.DataFrame(columns=columns)

engine = create_engine('oracle+cx_oracle://c##MILAN:milkan980@127.0.0.1:1522/baza')
conn = engine.connect()

# A simple function acting as a Subscription listener
def on_prices_update(item_update):
    #item_update['pos'] item_update['name'] item_update['values']['BID_OPEN']

     if item_update['name'] == 'CHART:IX.D.DOW.DAILY.IP:1MINUTE':
        obradi_poruku(item_update,'DOW',False)
     if item_update['name'] == 'CHART:IX.D.DAX.DAILY.IP:1MINUTE':
         obradi_poruku(item_update,'DAX',False)
     if item_update['name'] == 'CHART:IX.D.FTSE.DAILY.IP:1MINUTE':
         obradi_poruku(item_update,'FTSE',False)
     if item_update['name'] == 'CHART:CS.D.GBPUSD.TODAY.IP:1MINUTE':
         obradi_poruku(item_update,'GBPUSD',True)
     if item_update['name'] == 'CHART:CS.D.EURUSD.TODAY.IP:1MINUTE':
         obradi_poruku(item_update,'EURUSD',True)
     if item_update['name'] == 'CHART:CS.D.USDJPY.TODAY.IP:1MINUTE':
         obradi_poruku(item_update,'USDJPY',True)

     #print("price: %s " % item_update)
    #print("{stock_name:<19}: Time {UPDATE_TIME:<8} - "
     #     "Bid {BID:>5} - Ask {OFFER:>5}".format(
      #        stock_name=item_update["name"], **item_update["values"]
       #   ))



def on_account_update(balance_update):
    print("balance: %s " % balance_update)


def main():
    logging.basicConfig(level=logging.INFO)
    # logging.basicConfig(level=logging.DEBUG)

    ig_service = IGService(
        config.username, config.password, config.api_key, config.acc_type
    )

    ig_stream_service = IGStreamService(ig_service)
    ig_session = ig_stream_service.create_session()
    # Ensure configured account is selected
    accounts = ig_session[u'accounts']
    for account in accounts:
        if account[u'accountId'] == config.acc_number:
            accountId = account[u'accountId']
            break
        else:
            print('Account not found: {0}'.format(config.acc_number))
            accountId = None
    ig_stream_service.connect(accountId)

    # Making a new Subscription in MERGE mode
    subscription_prices = Subscription(
        mode="MERGE",
        items=['CHART:IX.D.DOW.DAILY.IP:1MINUTE','CHART:IX.D.DAX.DAILY.IP:1MINUTE','CHART:IX.D.FTSE.DAILY.IP:1MINUTE',"CHART:CS.D.GBPUSD.TODAY.IP:1MINUTE","CHART:CS.D.EURUSD.TODAY.IP:1MINUTE","CHART:CS.D.USDJPY.TODAY.IP:1MINUTE"],
        #'CHART:IX.D.DOW.DAILY.IP:SECOND','CHART:IX.D.DAX.DAILY.IP:SECOND',
        fields=["UTM","OFR_OPEN","OFR_HIGH","OFR_LOW","OFR_CLOSE", "BID_OPEN","BID_HIGH","BID_LOW","BID_CLOSE","CONS_END","TTV"],
        #fields=["UPDATE_TIME", "BID", "OFFER", "CHANGE", "MARKET_STATE"],

    )
    # adapter="QUOTE_ADAPTER")

    # Adding the "on_price_update" function to Subscription
    subscription_prices.addlistener(on_prices_update)

    # Registering the Subscription
    sub_key_prices = ig_stream_service.ls_client.subscribe(subscription_prices)

    # Making an other Subscription in MERGE mode
    subscription_account = Subscription(
        mode="MERGE",
        items=['ACCOUNT:'+accountId],
        fields=["AVAILABLE_CASH"],
    )
    #    #adapter="QUOTE_ADAPTER")



    # Adding the "on_balance_update" function to Subscription
    subscription_account.addlistener(on_account_update)

    # Registering the Subscription
    sub_key_account = ig_stream_service.ls_client.subscribe(
        subscription_account
    )

    input("{0:-^80}\n".format(""
                              " TO UNSUBSCRIBE AND DISCONNECT FROM \
    LIGHTSTREAMER"))

    # Disconnecting
    ig_stream_service.disconnect()


def obradi_poruku(item_update,sifra,samo_minut):
    if int(item_update['values']['CONS_END']) == 1:
        obradi_tik(item_update, sifra)
        obradi_presek_minut(item_update, sifra)
    elif samo_minut == False:
        obradi_tik(item_update, sifra)

def obradi_tik(item_update,sifra):
    global df_tik
    size_pre = len(df_tik.index)
    try:
        # ['datum','datum_unosa','sifra','bid','ask'] ["UTM","OFR_OPEN","OFR_HIGH","OFR_LOW","OFR_CLOSE", "BID_OPEN","BID_HIGH","BID_LOW","BID_CLOSE","CONS_END"]
        df_tik.loc[size_pre + 1, 'datum'] = pd.to_datetime(item_update['values']['UTM'], infer_datetime_format=True,unit='ms')
        df_tik.loc[size_pre + 1, 'datum_unosa'] = pd.to_datetime(time.time() * 1000, unit='ms')
        df_tik.loc[size_pre + 1, 'sifra'] = sifra
        df_tik.loc[size_pre + 1, 'bid'] = float(item_update['values']['BID_CLOSE'])
        df_tik.loc[size_pre + 1, 'ask'] = float(item_update['values']['OFR_CLOSE'])
    #    print(df_tik)
    except BaseException:
        print("greska u podacima: %s " % item_update)
        if size_pre < len(df_tik.index):
            df_tik = df_tik[:-1]
    if len(df_tik.index) == 5:
        df_tik.to_sql('TIK', conn,dtype={"datum_unosa": TIMESTAMP}, if_exists='append', index=False)
     #   print("ucitan tik:" + df_tik['sifra'][1])
        df_tik = df_tik.iloc[0:0]

def obradi_presek_minut(item_update,sifra):
    global df_presek_minut
    size_pre = len(df_presek_minut.index)
    try:
        # ['datum','sifra','op','cl','hi','lo','cl','vol'] ["UTM","OFR_OPEN","OFR_HIGH","OFR_LOW","OFR_CLOSE", "BID_OPEN","BID_HIGH","BID_LOW","BID_CLOSE","CONS_END"]
        df_presek_minut.loc[size_pre + 1, 'datum'] = pd.to_datetime(item_update['values']['UTM'], infer_datetime_format=True,unit='ms')
        df_presek_minut.loc[size_pre + 1, 'sifra'] = sifra
        df_presek_minut.loc[size_pre + 1, 'op'] = (float(item_update['values']['BID_OPEN']) + float(item_update['values']['OFR_OPEN']))/2
        df_presek_minut.loc[size_pre + 1, 'cl'] = (float(item_update['values']['BID_CLOSE']) + float(item_update['values']['OFR_CLOSE']))/2
        df_presek_minut.loc[size_pre + 1, 'hi'] = (float(item_update['values']['BID_HIGH']) + float(item_update['values']['OFR_HIGH'])) / 2
        df_presek_minut.loc[size_pre + 1, 'lo'] = (float(item_update['values']['BID_LOW']) + float(item_update['values']['OFR_LOW'])) / 2
        df_presek_minut.loc[size_pre + 1, 'vol'] = pd.to_numeric(item_update['values']['TTV'], errors='coerce')
     #   print(df_presek_minut)
    except BaseException:
        print("greska u podacima: %s " % item_update)
        if size_pre < len(df_presek_minut.index):
            df_presek_minut = df_presek_minut[:-1]

    if len(df_presek_minut.index) == 1:
        df_presek_minut.to_sql('PRESEK_MINUT', conn, if_exists='append', index=False)
        print("ucitan 1 minut:" + df_presek_minut['sifra'][1])
        df_presek_minut = df_presek_minut.iloc[0:0]

if __name__ == '__main__':
    main()


#C:\Users\Milan\PycharmProjects\akcije>pyinstaller ig_stream\sample\stream_ig.py -F
#pip install --upgrade setuptools --user

