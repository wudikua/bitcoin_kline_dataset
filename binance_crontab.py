# encoding=utf8
import argparse
import subprocess
import time
import pandas as pd
import numpy as np
import os
import data_loader
from sqlalchemy import create_engine
import requests
import zlib

def downlaod(symbol_type, symbol_interval, symbol_task_no, total_task_num, proxy_host, proxy_port, mysql_db_url, update_type='update_kline'):
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'start download {} interval {}'.format(symbol_type, symbol_interval))
    if proxy_port >0 and len(proxy_host)>0:
        data_loader.set_up_proxy(proxy_host, proxy_port, True)
        print(data_loader.proxies)
        print(requests.get('https://ipinfo.io', proxies=data_loader.proxies).text)
    
    ttype = symbol_type
    interval = symbol_interval

    exchange_info = data_loader.get_info()

    trading_symbols = exchange_info[ttype][exchange_info[ttype]['status']=='TRADING']
    if 'heyue' in ttype:
        trading_symbols = trading_symbols[trading_symbols['contractType']=='PERPETUAL'].symbol.values
    else:
        trading_symbols = trading_symbols.symbol.values

    db_engine = create_engine(mysql_db_url)
    db_symbols = data_loader.get_db_symbols(db_engine)

    for symbol in trading_symbols:
        if total_task_num>0:
            if zlib.crc32(bytes(symbol, encoding='utf-8')) % total_task_num != symbol_task_no:
                continue
        if update_type == 'update_kline':
            data_loader.auto_update_data(symbol, interval, exchange_info, ttype, db_symbols, db_engine)
        elif update_type == 'update_longshort':
            data_loader.auto_update_heyue_longshort_data(symbol, interval, exchange_info, ttype, db_engine)
        
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '{} finish download {} interval {}'.format(update_type, symbol_type, symbol_interval))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='binance data downloader')
    parser.add_argument('--action', type=str, default='help', help='help/download')
    parser.add_argument('--symbol_type', type=str, default='', help='xianhuo/bi_heyue/u_heyue')
    parser.add_argument('--symbol_interval', type=str, default='', help='1d/1h/15m')
    parser.add_argument('--symbol_task_no', type=int, default=-1, help='task index')
    parser.add_argument('--symbol_split_num', type=int, default=-1, help='n tasks')
    parser.add_argument('--proxy_host', type=str, default='', help='proxy_host')
    parser.add_argument('--proxy_port', type=int, default=-1, help='proxy_port')
    parser.add_argument('--mysql_db_url', type=str, default='mysql+pymysql://root:mengjun1990@localhost:3306/findata', help='mysql connection string')
    args = parser.parse_args()
    
    task_map = {
        "task1": [('xianhuo', '1d')],
        "task2": [('u_heyue', '1d')],
        "task3": [('bi_heyue', '1d')],
        
        "task4": [('xianhuo', '1h')],
        "task5": [('u_heyue', '1h')],
        "task6": [('bi_heyue', '1h')],
        
        "task7": [('xianhuo', '15m')],
        "task8": [('u_heyue', '15m')],
        "task9": [('bi_heyue', '15m')],
    }
    
    if args.action == 'help':
        parser.print_help()
    elif args.action == 'download_all':
        if len(args.proxy_host)>0 and args.proxy_port>0:
            data_loader.set_up_proxy(args.proxy_host, args.proxy_port, True)
            print(data_loader.proxies)
            print(requests.get('https://ipinfo.io', proxies=data_loader.proxies).text)
        
        exchange_info = data_loader.get_info()
        db_engine = create_engine(args.mysql_db_url)
        data_loader.insert_or_update_exchange_info(db_engine, exchange_info)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'update exchange info')
        
        pids = []
        for k in task_map.keys():
            t = []
            i = []
            for pair in task_map[k]:
                t.append(pair[0])
                i.append(pair[1])
            
            
            if args.symbol_split_num < 0:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'parent program execute subprocess download {} interval {}'.format(','.join(t), ','.join(i)))
                pids.append(subprocess.Popen(['python', '-u', __file__, 
                                              '--action=download_symbol_type_interval', '--symbol_type={}'.format(','.join(t)), 
                                              '--symbol_interval={}'.format(','.join(i))
                                             ]))
            elif args.symbol_split_num>0:
                for task_n in range(args.symbol_split_num):
                    
                    subprocess_args = ['python', '-u', __file__, 
                                      '--action=download_symbol_type_interval', '--symbol_type={}'.format(','.join(t)), 
                                      '--symbol_interval={}'.format(','.join(i)),
                                      '--symbol_task_no={}'.format(task_n),
                                      '--symbol_split_num={}'.format(args.symbol_split_num),
                                      '--mysql_db_url={}'.format(args.mysql_db_url),
                                      '--proxy_host={}'.format(args.proxy_host),
                                      '--proxy_port={}'.format(args.proxy_port),
                                     ]
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'parent program execute subprocess ', ' '.join(subprocess_args))
                    pids.append(subprocess.Popen(subprocess_args))
        for pid in pids:
            try:
                print(pid, pid.wait())
            except Exception as e:
                print(pid, 'timeout')
    elif args.action == 'download_symbol_type_interval':
        symbol_type = args.symbol_type
        symbol_interval = args.symbol_interval
        if symbol_type == '' or symbol_interval == '':
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'download error, check symbol type or interval')
        for t in symbol_type.split(","):
            for i in symbol_interval.split(","):
                downlaod(t, i, args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url)
    elif args.action == 'download_symbol_longshort_type_interval':
        downlaod('bi_heyue', '1d', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')
        downlaod('bi_heyue', '1h', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')
        downlaod('bi_heyue', '15m', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')
        downlaod('u_heyue', '1d', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')
        downlaod('u_heyue', '1h', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')
        downlaod('u_heyue', '15m', args.symbol_task_no, args.symbol_split_num, args.proxy_host, args.proxy_port, args.mysql_db_url, update_type='update_longshort')