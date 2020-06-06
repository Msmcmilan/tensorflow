import cx_Oracle
import csv
import datetime
import os
import pandas as pd
import time
##import sqlalchemy
from sqlalchemy import types, create_engine,MetaData,Table,Column,Integer,String,Date,Numeric,text,Sequence

connection = cx_Oracle.connect('c##MILAN/milkan980@127.0.0.1:1522/baza')
cursor = connection.cursor()

engine = create_engine('oracle+cx_oracle://c##MILAN:milkan980@127.0.0.1:1522/baza')
conn = engine.connect()
#769904700
sifra = 'DOW'

presek = engine.execute("SELECT SIFRA, DATUM, OP, HI, LO, CL, VOL FROM PRESEK_MINUT "
                        "where datum >= to_date('1/03/2020 00:00:00', 'DD/MM/YYYY HH24:MI:SS') and "
                        "datum < to_date('26/03/2020 00:00:00', 'DD/MM/YYYY HH24:MI:SS') and "
                        "SIFRA like '"+sifra+"'"
                         " order by datum asc").fetchall()

metadata = MetaData()

prozor_tabela = Table('PROZOR', metadata,
    Column('id_prozor', Integer,Sequence('prozor_seq'),primary_key=True),
    Column('sifra', String),
    Column('datum', Date),
    Column('datum_chart', Date),
    Column('redni_broj_prozora',Integer),
    Column('op', Numeric(18,5)),
    Column('hi', Numeric(18,5)),
    Column('lo', Numeric(18,5)),
    Column('cl', Numeric(18,5)),
    Column('vol',Integer),
)
prozor_rezultat_tabela = Table('PROZOR_REZULTAT', metadata,
    Column('redni_broj_prozora',Integer),
    Column('sifra', String),
    Column('rezultat', Numeric(18,5))
)
delz = text("delete from prozor where sifra = :v1")
delz_rezultat = text("delete from prozor_rezultat where sifra = :v1")
delz_prozora = text("delete from prozor where sifra = :v1 and redni_broj_prozora = :v2")
#conn.execute(delz, v1=sifra)
#conn.execute(delz_rezultat, v1=sifra)

ins = text("INSERT INTO PROZOR (ID_PROZOR,SIFRA,DATUM,DATUM_CHART,REDNI_BROJ_PROZORA,OP,HI,LO,CL,VOL) VALUES (prozor_seq.nextval,:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9)")
##ins = text("INSERT INTO PROZOR (ID_PROZOR,ID_SIMBOL,DATUM_OD) VALUES (prozor_seq.nextval,:v1,:v2)")

strategija = engine.execute("select * from strategija where id_strategija = 1").fetchone()

def obrisi_prozor(redni_broj,sifra):
    conn.execute(delz_prozora, v1=sifra,v2=redni_broj)
    return;

redni_broj_prozora = engine.execute("select max(redni_broj_prozora) as rb from prozor where sifra = '" +sifra+ "'").fetchone()["rb"]

velicina_prozora = 60
velicina_nastavka = 10
stop = 0
limit = 0

##datum_do_prozora = datetime.datetime(1900, 1, 1, 0, 0, 0)
broj_tikova = 0

prethodni_datum = datetime.datetime(1900, 1, 1, 0, 0, 0)
for row1 in presek:
    print("pocetak " + str(row1['datum']))

    #print((row1['datum'] - prethodni_datum).days)
    #print((row1['datum'] - prethodni_datum).seconds)
    #prethodni_datum = row1['datum']
    #continue
    datum_chart = datetime.date(2000, 1, 1)
    prethodni_datum = datetime.datetime(1900, 1, 1, 0, 0, 0)
    dani_razmaka = 0
    razmak_sekunde = 0
    razmak_dani = 0
    postoji_greska = 0
    broj_napravljenih = 0
    prozor = engine.execute(
        "SELECT SIFRA, DATUM, OP, HI, LO, CL, VOL FROM PRESEK_MINUT where datum >= :datum and SIFRA like '"+sifra+"'"
        " order by datum asc "
        " FETCH FIRST "+str(velicina_prozora + velicina_nastavka)+" ROWS ONLY",datum=row1['datum']).fetchall()
    if len(prozor) < velicina_prozora + velicina_nastavka:
        break
    pocetna_vrednost = row1['op']
    redni_broj_prozora = redni_broj_prozora + 1
    for row2 in prozor:
        razmak_dani = (row2['datum'] - prethodni_datum).days
        razmak_sekunde = (row2['datum'] - prethodni_datum).seconds
        if prethodni_datum != datetime.datetime(1900, 1, 1, 0, 0, 0):
            dani_razmaka = dani_razmaka + razmak_dani
            if dani_razmaka > 2 and prethodni_datum != datetime.datetime(1900, 1, 1, 0, 0, 0):
                postoji_greska = 1

            if razmak_sekunde != 60 and razmak_sekunde != 63060 and razmak_sekunde != 63000 and prethodni_datum != datetime.datetime(1900, 1, 1, 0, 0, 0):
                postoji_greska = 1

            if postoji_greska == 1:
                obrisi_prozor(redni_broj_prozora, row2['sifra'])
                redni_broj_prozora = redni_broj_prozora - 1
                break

            # datum za chart
            if razmak_sekunde == 60:
                datum_chart = datum_chart + datetime.timedelta(days=1)
            # razmak kad je novi dan ili nova nedelja
            if (razmak_sekunde == 63060 or razmak_sekunde == 63000):
                datum_chart = datum_chart + datetime.timedelta(days=2)
            if (razmak_dani == 2):
                datum_chart = datum_chart + datetime.timedelta(days=4)


        ##print(redni_broj_prozora)
        ##print(datum_chart)
        ins_prozor = prozor_tabela.insert().values( sifra=row2['sifra'],datum=row2['datum'],datum_chart=datum_chart,redni_broj_prozora=redni_broj_prozora,op=row2['op']/pocetna_vrednost,hi=row2['hi']/pocetna_vrednost,lo=row2['lo']/pocetna_vrednost,cl=row2['cl']/pocetna_vrednost,vol=row2['vol'])
        ##conn.execute(ins, v1=row2['id_simbol'],v2=row2['datum'],v3=datum_chart,v4=redni_broj_prozora,v5=row2['op']/pocetna_vrednost,v6=row2['hi']/pocetna_vrednost,v7=row2['lo']/pocetna_vrednost,v8=row2['cl']/pocetna_vrednost,v9=row2['vol'])
        ##conn.execute("commit")
        result = conn.execute(ins_prozor)
        ##print(row2['datum'])
        broj_napravljenih = broj_napravljenih + 1
       ## print(result.inserted_primary_key)

        prethodni_datum = row2['datum']

        if broj_napravljenih == velicina_prozora:
            uzeli_na = row2['cl']
            stop_gore = row2['cl'] + row2['cl']*strategija['p_za_zatvaranje']
            stop_dole = row2['cl'] - row2['cl']*strategija['p_za_stop']
            rezultat = 0
            for i in range(0, velicina_nastavka):
                ##print(prozor[velicina_prozora + i])
                if stop_gore <= prozor[velicina_prozora + i]['hi']:
                    if stop_dole >= prozor[velicina_prozora + i]['lo']:
                        print("stop_gore_dole -"+str(prozor[velicina_prozora + i]['datum']))
                        rezultat = 0
                        break
                    else:
                        print("stop_gore -" + str(prozor[velicina_prozora + i]['datum']))
                        rezultat = 1
                        break
                if stop_dole >= prozor[velicina_prozora + i]['lo']:
                    print("stop_dole -"+str(prozor[velicina_prozora + i]['datum']))
                    rezultat = 2
                    break
                if i == velicina_nastavka - 1:
                    print("nema_stop")

            ins_rezultat = prozor_rezultat_tabela.insert().values(
                sifra=sifra,
                redni_broj_prozora=redni_broj_prozora,
                rezultat=rezultat)
            result = conn.execute(ins_rezultat)
            break
        #print(dani_razmaka)
        ##print(datum_od_prozora)
        ##print(datum_do_prozora)
        ##print(datum)
        ##conn.execute(ins, v1=row1['id_simbol'], v2=row1['datum'],)



cursor.close()
connection.close()