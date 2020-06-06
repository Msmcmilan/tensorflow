import pandas as pd

from sqlalchemy import create_engine,text


def insert_u_presek(df,sifra,naziv_tabele):
    #df kolone sifra,datum,op,cl,hi,lo,vol

    if 'datum' not in df.columns:
        raise Exception("Ne postoji kolona datum")
    if 'op' not in df.columns:
        raise Exception("Ne postoji kolona op")
    if 'cl' not in df.columns:
        raise Exception("Ne postoji kolona cl")
    if 'hi' not in df.columns:
        raise Exception("Ne postoji kolona hi")
    if 'lo' not in df.columns:
        raise Exception("Ne postoji kolona lo")
    #if 'vol' not in df.columns:
    #    print("Ne postoji kolona vol")
    engine = create_engine('oracle+cx_oracle://c##MILAN:milkan980@127.0.0.1:1522/baza')
    conn = engine.connect()

   # for index, row2 in df.iterrows():
     #   df.loc[index, 'id_presek'] = engine.execute("select presek_30_seq.nextval from dual").fetchone()[0]
    df['sifra'] = sifra
    df = df.rename(columns={"timestamp": "datum","Open": "op", "open": "op", "close": "cl", "high": "hi", "low": "lo",
                          "volume": "vol"})
    df['datum'] = pd.to_datetime(df['datum'], infer_datetime_format=True)
    ## print(c)
    tabela = ''
    df.to_sql(naziv_tabele, conn, if_exists='append', index=False)

    delz = text("delete from "+naziv_tabele+" where OP is null")

    conn.execute(delz)

    conn.close()