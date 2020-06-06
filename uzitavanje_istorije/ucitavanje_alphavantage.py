import pandas as pd

from rad_sa_bazom import dataframe_u_bazu

sifra = 'IBM'
url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol="+sifra+"&interval=1min&apikey=MB1XSRUN4EUAGQDL&datatype=csv&outputsize=full"
c = pd.read_csv(url)
print(url)
print(len(c.index))
if len(c.index) > 300:
    c = c.rename(columns={"timestamp": "datum","Open": "op", "open": "op", "close": "cl", "high": "hi", "low": "lo",
                          "volume": "vol"})
    dataframe_u_bazu.insert_u_presek(c, sifra,"PRESEK_MINUT")
