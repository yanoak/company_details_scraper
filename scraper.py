import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from time import sleep
from random import randint
import sqlite3 as sql

links = pd.read_csv('dica_links.csv', header=None)

for url in links[0]:
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    t1 = soup.find_all('div', attrs={'class':'field-items'})
    t1_data = [x.findChildren(text=True) for x in t1]    
    t2 = soup.find_all('td')
    t2_data = [y.findChildren(text=True) for y in t2]
    t2_data = [[u"".join(a).strip() for a in z]for z in t2_data]
    records = []

    name = None
    name_mm = None
    reg_no = None
    reg_date = None
    exp_date = None
    addr = None

    try:
        name = t1_data[0]
        name_mm = t1_data[1]
        reg_no = t1_data[2]
        reg_date = t1_data[3]
        exp_date = t1_data[4]
        addr = t1_data[5]
    except:
        pass


    #Need to Clean up Variables due to Serious data inconsistency in the tables
    dname_1 = None
    dtype_1 = None
    dnrc_1 = None
    dname_2 = None
    dtype_2 = None
    dnrc_2 = None
    dname_3 = None
    dtype_3 = None
    dnrc_3 = None



    #try except to avoid Index Error
    try:
        dname_1 = t2_data[0]
        dtype_1 = t2_data[1]
        dnrc_1 = t2_data[2]
        dname_2 = t2_data[5]
        dtype_2 = t2_data[6]
        dnrc_2 = t2_data[7]
        dname_3 = t2_data[10]
        dtype_3 = t2_data[11]
        dnrc_3 = t2_data[12]
    except:
        pass

    records.append((name, name_mm, reg_no, reg_date,exp_date,addr,dname_1,dtype_1,dnrc_1, dname_2, dtype_2, dnrc_2,dname_3, dtype_3,dnrc_3))



    df = pd.DataFrame(records, columns=['name', 'name_mm', 'reg_no', 'reg_date','exp_date','addr','dname_1','dtype_1','dnrc_1', 'dname_2', 'dtype_2', 'dnrc_2','dname_3', 'dtype_3','dnrc_3'])


    sql_con = sql.connect("data.sqlite")
    df.to_sql(name='data',con=sql_con ,if_exists='append')
    for _ in range(0,3): #to control the crawl rate
        print('.')
        sleep(randint(1,3))
    



