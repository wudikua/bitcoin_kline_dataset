# encoding=utf8
import pandas as pd
import numpy as np
import os
import data_loader
from sqlalchemy import create_engine
import requests

data_loader.set_up_proxy('192.168.1.3', 11223, True)
print(data_loader.proxies)

print(requests.get('https://ipinfo.io', proxies=data_loader.proxies).text)

ttype = 'xianhuo'
interval = '15m'


exchange_info = data_loader.get_info()

trading_symbols = exchange_info[ttype][exchange_info[ttype]['status']=='TRADING']
if 'heyue' in ttype:
    trading_symbols = trading_symbols[trading_symbols['contractType']=='PERPETUAL'].symbol.values
else:
    trading_symbols = trading_symbols.symbol.values

db_engine = create_engine('mysql+pymysql://root:mengjun1990@localhost:3306/findata')
db_symbols = data_loader.get_db_symbols(db_engine)

for symbol in trading_symbols:
    data_loader.auto_update_data(symbol, interval, exchange_info, ttype, db_symbols, db_engine)
    
    
    
