################################################################################################
#	name:	timeseries_OHLC.py
#	desc:	creates OHLC graph
#	date:	2018-06-15
#	Author:	conquistadorjd
################################################################################################
import pandas as pd
# import pandas_datareader as datareader
import matplotlib.pyplot as plt
import datetime
from mpl_finance import candlestick_ohlc
# from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import os
from sqlalchemy import types, create_engine


engine = create_engine('oracle+cx_oracle://c##MILAN:milkan980@127.0.0.1:1522/baza')
conn = engine.connect()

print('*** Program Started ***')

##df = pd.read_csv('15-06-2016-TO-14-06-2018HDFCBANKALLN.csv')
url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=30min&apikey=MB1XSRUN4EUAGQDL&datatype=csv&outputsize=full"

sifra = "DOW"

presek = engine.execute("SELECT prozor.redni_broj_prozora,prozor_rezultat.rezultat from prozor "
                        "inner join prozor_rezultat on (prozor_rezultat.sifra = prozor.sifra and prozor_rezultat.redni_broj_prozora = prozor.redni_broj_prozora) "
                        "where prozor.SIFRA like '"+sifra+"' "
                        "and prozor.redni_broj_prozora > 2569 "
                        "group by prozor.redni_broj_prozora,prozor_rezultat.rezultat order by prozor.redni_broj_prozora"
                         ).fetchall()

train_number = int(len(presek)*0.75)
validation_number =len(presek) - train_number
obradjenih_prozora = 0
folder = 'C:\\tensor\\test'

if not os.path.exists(folder):
    os.mkdir(folder)
if not os.path.exists(folder + "\\train"):
    os.mkdir(folder + "\\train")
if not os.path.exists(folder + "\\validation"):
    os.mkdir(folder + "\\validation")


for row1 in presek:


    df = pd.read_sql_query("select prozor.datum_chart as timestamp,prozor.cl as open,prozor.hi as high,prozor.lo as low,prozor.op as close from prozor "
                           "where prozor.redni_broj_prozora = " + str(row1["redni_broj_prozora"]) + " and prozor.sifra = '" + sifra + "' order by prozor.datum asc",conn)

    # ensuring only equity series is considered
    ##df = df.loc[df['Series'] == 'EQ']

    # Converting date to pandas datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df["timestamp"] = df["timestamp"].apply(mdates.date2num)

    # Creating required data in new DataFrame OHLC
    ohlc= df[['timestamp', 'open', 'high', 'low','close']].copy()
    # In case you want to check for shorter timespan

    ohlc =ohlc.head(96)

    f1, ax = plt.subplots(figsize = (5,2.5))
    ax.set_ylim(0.945,1.055)

    # plot the candlesticks
    candlestick_ohlc(ax, ohlc.values, width=1/2, colorup='red', colordown='green')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    # Saving image
    putanja = ''
    naziv = "prozor_" + str(row1["rezultat"]) + "_" + str(row1["redni_broj_prozora"]) + ".png"
    if obradjenih_prozora < train_number:
        putanja = folder+"\\train\\rezultat_" + str(row1["rezultat"])
        if not os.path.exists(putanja):
            os.mkdir(putanja)
    else:
        putanja = folder + "\\validation\\rezultat_" + str(row1["rezultat"])
        if not os.path.exists(putanja):
            os.mkdir(putanja)

    if os.path.exists(putanja+"\\"+naziv):
        os.remove(putanja+"\\"+naziv)
    plt.savefig(putanja+"\\"+naziv,format = "png")
    obradjenih_prozora = obradjenih_prozora + 1
    plt.cla()
    # Clear the current figure.
    plt.clf()
    # Closes all the figure windows.
    plt.close('all')

    # In case you dont want to save image but just displya it
    #plt.show()


    """
    f1 = plt.subplot2grid((6, 4), (1, 0), rowspan=6, colspan=4)
    candlestick_ohlc(f1, ohlc.values, width=1/48, colorup='green', colordown='red')
    f1.xaxis_date()
    f1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d %H:%M:%S'))
    
    plt.xticks(rotation=45)
    plt.ylabel('Stock Price')
    plt.xlabel('Date Hours:Minutes')
    plt.show()
    """
    print(putanja+"\\"+naziv)
print("kraj!!!")