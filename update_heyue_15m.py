# encoding=utf8

import pandas as pd
import numpy as np
import os
import data_loader
import math
import time
import datetime

exchange_info = data_loader.get_info()

trading_symbols = exchange_info['u_heyue'][exchange_info['u_heyue']['status']=='TRADING']
trading_symbols = trading_symbols[trading_symbols['contractType']=='PERPETUAL'].symbol.values

for symbol in trading_symbols:
    path = '/root/bt_fund/data/base/u_heyue/15m/{}_u_heyue_15m.csv'.format(symbol)
    if not os.path.exists(path):
        symbol_info = exchange_info['u_heyue'][exchange_info['u_heyue']['symbol']==symbol]
        
        total_trading_days = math.ceil((time.time()*1000-symbol_info.onboardDate.values[0])/1000/3600/24)
        
        df = data_loader.download_symbol_historty_kline(symbol, '15m', 
                                                        '{} day ago UTC'.format(min(total_trading_days, 1000)), 
                                                        '0 day ago UTC', conf_type='u_heyue', maxLimit=1500)
        if df.shape[0] == 0:
            print(symbol, ' is empty')
        else:
            df.to_csv(path, index=None, compression='bz2')
            print('download ', symbol)
    else:
        df = pd.read_csv(path, compression='bz2')
        latest_row = df.iloc[-1:]
        latest_ts = latest_row.close_ts.values[0]
        delta_seconds = (math.floor(time.time()*1000) - latest_ts)/1000
        delta_minutes = math.ceil(delta_seconds/60/15)
        query_start_minutes = delta_minutes + 1
        incr_df = data_loader.download_symbol_historty_kline(symbol, '15m', '{} hour ago UTC'.format(query_start_minutes), '0 hour ago UTC', conf_type='u_heyue', maxLimit=1500)
        print('{} incremental download from {} peroids ago'.format(symbol, delta_minutes))

        # 合并两个集合
        merge_ts = df.iloc[-2:-1].close_ts.values[0]
        new_incr_df = incr_df[incr_df.close_ts>merge_ts]
        new_df = pd.concat([df.iloc[:-1].reset_index(drop=True), new_incr_df.reset_index(drop=True)], axis=0).reset_index(drop=True)
        new_df.to_csv(path, index=None, compression='bz2')
        print('{} incremental update {} periods'.format(symbol, new_incr_df.shape[0]))
