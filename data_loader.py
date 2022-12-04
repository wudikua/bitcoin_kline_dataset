# coding=utf-8

import pandas as pd
import numpy as np
import os
import json
import requests
import time
from binance.helpers import convert_ts_str
import math
import datetime

BINANCE_KEY = {
    'binance-api-key': 'B9UGufN3AUKLFMqYaOvVagKHNKbcdpnX8kqM6gjAUte5gbgkVN4BUEjBlZDGr65U',
    'binance-api-secret': 'a3NOHCMx06zJevYmVvDmVOdimvgpoUqhV1yfFnKDkdhuCR2VcvXOr6s03JgDzMR6'
}

API_CONF = {
    'xianhuo': {'host': 'api.binance.com', 'kline': '/api/v3/klines', 'kline_limit': 1000, 'info': '/api/v3/exchangeInfo'},
    'u_heyue': {'host': 'fapi.binance.com', 'kline': '/fapi/v1/klines'
                , 'taker_long_short_ratio':'/futures/data/takerlongshortRatio'
                , 'global_long_short_account_ratio':'/futures/data/globalLongShortAccountRatio'
                , 'top_long_short_position_ratio':'/futures/data/topLongShortPositionRatio'
                , 'top_long_short_account_ratio':'/futures/data/topLongShortAccountRatio'
                , 'open_interest_hist':'/futures/data/openInterestHist'
                , 'fund_rate': '/fapi/v1/fundingRate', 'kline_limit': 1500, 'info': '/fapi/v1/exchangeInfo'},
    'bi_heyue': {'host': 'dapi.binance.com', 'kline': '/dapi/v1/klines'
                , 'jicha':'/futures/data/basis'
                , 'taker_buy_sell_vol':'/futures/data/takerBuySellVol'
                , 'global_long_short_account_ratio':'/futures/data/globalLongShortAccountRatio'
                , 'top_long_short_position_ratio':'/futures/data/topLongShortPositionRatio'
                , 'top_long_short_account_ratio':'/futures/data/topLongShortAccountRatio'
                , 'open_interest_hist':'/futures/data/openInterestHist'
                , 'fund_rate': '/dapi/v1/fundingRate', 'kline_limit': 200, 'info': '/dapi/v1/exchangeInfo'},
    'qiquan': {'host': 'eapi.binance.com', 'info': '/eapi/v1/exchangeInfo'}
}

proxies = {
}


def set_up_proxy(host, port, is_socks5=True):
    if is_socks5:
        proxies['http'] = 'socks5h://{}:{}'.format(host, port)
        proxies['https'] = 'socks5h://{}:{}'.format(host, port)
    else:
        proxies['http'] = 'http://{}:{}'.format(host, port)
        proxies['https'] = 'https://{}:{}'.format(host, port)


def binance_request(uri, host='api.binance.com', header=None, retry=10):
    if header is None:
        header = {}
    for i in range(retry):
        try:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'request ', 'https://{}{}'.format(host, uri),
                  header)
            rt = requests.get('https://{}{}'.format(host, uri), header, proxies=proxies, verify=True)
            if rt.status_code == 200:
                data = json.loads(rt.text)
                if 'code' in data and data['code'] == 429:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求被限速')
                    time.sleep(i * 3)
                    continue
                # print('request succ')
                return rt.status_code, data, rt.headers
            else:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '响应结果非200 {} {}'.format(rt.status_code, rt.text))
        except Exception as e:
            print('catch exception ', e)
        time.sleep(i * 3)
    return 0, None, None


def get_info():
    _, info_xianhuo, _ = binance_request('/api/v3/exchangeInfo')

    df_xianhuo = pd.DataFrame(info_xianhuo['symbols'])
    df_xianhuo = df_xianhuo[['symbol', 'status', 'baseAsset', 'quoteAsset']]

    _, info_u_heyue, _ = binance_request(uri=API_CONF['u_heyue']['info'], host=API_CONF['u_heyue']['host'])
    df_u_heyue = pd.DataFrame(info_u_heyue['symbols'])
    df_u_heyue = df_u_heyue[
        ['symbol', 'status', 'baseAsset', 'quoteAsset', 'contractType', 'onboardDate', 'marginAsset', 'underlyingType',
         'liquidationFee']]

    _, info_bi_heyue, _ = binance_request(uri=API_CONF['bi_heyue']['info'], host=API_CONF['bi_heyue']['host'])
    df_bi_heyue = pd.DataFrame(info_bi_heyue['symbols'])
    df_bi_heyue = df_bi_heyue[
        ['symbol', 'contractStatus', 'baseAsset', 'quoteAsset', 'contractType', 'onboardDate', 'marginAsset',
         'underlyingType', 'liquidationFee']]
    df_bi_heyue['status'] = df_bi_heyue['contractStatus']

    _, info_qiquan, _ = binance_request(uri=API_CONF['qiquan']['info'], host=API_CONF['qiquan']['host'])
    df_qiquan = pd.DataFrame(info_qiquan['optionSymbols'])
    df_qiquan = df_qiquan[df_qiquan.columns.difference(['filters'])]

    return {'xianhuo': df_xianhuo, 'u_heyue': df_u_heyue, 'bi_heyue': df_bi_heyue, 'qiquan': df_qiquan}


def download_symbol_u_heyue_historty_short_long_ratio(symbol, interval, start_str, end_str, maxLimit=500):
    conf_type='u_heyue'
    endTime = convert_ts_str(end_str)
    finalStartTime = convert_ts_str(start_str)
    if finalStartTime == endTime:
        return pd.DataFrame({})
    startTime = endTime

    open_interest_hist_result = []
    top_long_short_account_ratio_result = []
    top_long_short_position_ratio_result = []
    taker_long_short_ratio_result = []
    global_long_short_account_ratio_result = []
    while startTime >= finalStartTime:
        limit = maxLimit
        if interval == '1m':
            startTime = endTime - 60 * 1000 * limit
        elif interval == '15m':
            startTime = endTime - 15 * 60 * 1000 * limit
        elif interval == '1h':
            startTime = endTime - 60 * 60 * 1000 * limit
        elif interval == '1d':
            startTime = endTime - 24 * 60 * 60 * 1000 * limit

        startTime = max(startTime, finalStartTime)
        deltaTime = (endTime - startTime) / 1000
        if interval == '1m':
            limit = math.ceil(deltaTime / 60)
        elif interval == '15m':
            limit = math.ceil(deltaTime / (15 * 60))
        elif interval == '1h':
            limit = math.ceil(deltaTime / (60 * 60))
        elif interval == '1d':
            limit = math.ceil(deltaTime / (24 * 60 * 60))
        if limit <= 0:
            break
        status, info, headers = binance_request(uri=API_CONF[conf_type]['open_interest_hist'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        open_interest_hist_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['top_long_short_account_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        top_long_short_account_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['top_long_short_position_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        top_long_short_position_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['global_long_short_account_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        global_long_short_account_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['taker_long_short_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        taker_long_short_ratio_result.extend(info)
        
        
        endTime = startTime

    open_interest_hist_df = pd.DataFrame(open_interest_hist_result,
                      columns=['symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timestamp']).sort_values('timestamp')
    open_interest_hist_df['interval'] = interval
    open_interest_hist_df['datetime'] = open_interest_hist_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    open_interest_hist_df['market_cate1'] = '1000'
    open_interest_hist_df['market_cate2'] = cate2_conf[conf_type]
    open_interest_hist_df = open_interest_hist_df[['market_cate1', 'market_cate2', 'symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    top_long_short_account_ratio_df = pd.DataFrame(top_long_short_account_ratio_result,
                      columns=['symbol', 'longShortRatio', 'longAccount', 'shortAccount', 'timestamp']).sort_values('timestamp')
    top_long_short_account_ratio_df['interval'] = interval
    top_long_short_account_ratio_df['long'] = top_long_short_account_ratio_df['longAccount']
    top_long_short_account_ratio_df['short'] = top_long_short_account_ratio_df['shortAccount']
    top_long_short_account_ratio_df['datetime'] = top_long_short_account_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    top_long_short_account_ratio_df['market_cate1'] = '1000'
    top_long_short_account_ratio_df['market_cate2'] = cate2_conf[conf_type]
    top_long_short_account_ratio_df = top_long_short_account_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    top_long_short_position_ratio_df = pd.DataFrame(top_long_short_position_ratio_result,
                      columns=['symbol', 'longShortRatio', 'longAccount', 'shortAccount', 'timestamp']).sort_values('timestamp')
    top_long_short_position_ratio_df['interval'] = interval
    top_long_short_position_ratio_df['long'] = top_long_short_position_ratio_df['longAccount']
    top_long_short_position_ratio_df['short'] = top_long_short_position_ratio_df['shortAccount']
    top_long_short_position_ratio_df['datetime'] = top_long_short_position_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    top_long_short_position_ratio_df['market_cate1'] = '1000'
    top_long_short_position_ratio_df['market_cate2'] = cate2_conf[conf_type]
    top_long_short_position_ratio_df = top_long_short_position_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    global_long_short_account_ratio_df = pd.DataFrame(global_long_short_account_ratio_result,
                      columns=['symbol', 'longShortRatio', 'longAccount', 'shortAccount', 'timestamp']).sort_values('timestamp')
    global_long_short_account_ratio_df['interval'] = interval
    global_long_short_account_ratio_df['long'] = global_long_short_account_ratio_df['longAccount']
    global_long_short_account_ratio_df['short'] = global_long_short_account_ratio_df['shortAccount']
    global_long_short_account_ratio_df['datetime'] = global_long_short_account_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    global_long_short_account_ratio_df['market_cate1'] = '1000'
    global_long_short_account_ratio_df['market_cate2'] = cate2_conf[conf_type]
    global_long_short_account_ratio_df = global_long_short_account_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    taker_long_short_ratio_df = pd.DataFrame(taker_long_short_ratio_result,
                      columns=['buySellRatio', 'buyVol', 'sellVol', 'timestamp']).sort_values('timestamp')
    taker_long_short_ratio_df['symbol'] = symbol
    taker_long_short_ratio_df['interval'] = interval
    taker_long_short_ratio_df['buy'] = taker_long_short_ratio_df['buyVol']
    taker_long_short_ratio_df['sell'] = taker_long_short_ratio_df['sellVol']
    taker_long_short_ratio_df['buyValue'] = 0
    taker_long_short_ratio_df['sellValue'] = 0
    taker_long_short_ratio_df['datetime'] = taker_long_short_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    taker_long_short_ratio_df['market_cate1'] = '1000'
    taker_long_short_ratio_df['market_cate2'] = cate2_conf[conf_type]
    taker_long_short_ratio_df = taker_long_short_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'buySellRatio', 'buy', 'sell', 'buyValue', 'sellValue', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    
    
    return open_interest_hist_df, top_long_short_account_ratio_df, top_long_short_position_ratio_df, global_long_short_account_ratio_df, taker_long_short_ratio_df

def download_symbol_bi_heyue_historty_short_long_ratio(symbol, interval, start_str, end_str, maxLimit=500):
    conf_type='bi_heyue'
    endTime = convert_ts_str(end_str)
    finalStartTime = convert_ts_str(start_str)
    if finalStartTime == endTime:
        return pd.DataFrame({})
    startTime = endTime

    open_interest_hist_result = []
    top_long_short_account_ratio_result = []
    top_long_short_position_ratio_result = []
    global_long_short_account_ratio_result = []
    taker_buy_sell_vol_result = []
    jicha_result = []
    while startTime >= finalStartTime:
        limit = maxLimit
        if interval == '1m':
            startTime = endTime - 60 * 1000 * limit
        elif interval == '15m':
            startTime = endTime - 15 * 60 * 1000 * limit
        elif interval == '1h':
            startTime = endTime - 60 * 60 * 1000 * limit
        elif interval == '1d':
            startTime = endTime - 24 * 60 * 60 * 1000 * limit

        startTime = max(startTime, finalStartTime)
        deltaTime = (endTime - startTime) / 1000
        if interval == '1m':
            limit = math.ceil(deltaTime / 60)
        elif interval == '15m':
            limit = math.ceil(deltaTime / (15 * 60))
        elif interval == '1h':
            limit = math.ceil(deltaTime / (60 * 60))
        elif interval == '1d':
            limit = math.ceil(deltaTime / (24 * 60 * 60))
        if limit <= 0:
            break
        new_symbol = symbol
        if '_PERP' in new_symbol:
            new_symbol = new_symbol.replace('_PERP', '')
        status, info, headers = binance_request(uri=API_CONF[conf_type]['open_interest_hist'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit), 'contractType':'PERPETUAL'
                                                })
        if status != 200 or len(info) == 0:
            break
        open_interest_hist_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['top_long_short_account_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        top_long_short_account_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['top_long_short_position_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        top_long_short_position_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['global_long_short_account_ratio'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        global_long_short_account_ratio_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['taker_buy_sell_vol'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit), 'contractType':'PERPETUAL'
                                                })
        if status != 200 or len(info) == 0:
            break
        taker_buy_sell_vol_result.extend(info)
        
        status, info, headers = binance_request(uri=API_CONF[conf_type]['jicha'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'pair': str(new_symbol), 'period': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit), 'contractType':'PERPETUAL'
                                                })
        if status != 200 or len(info) == 0:
            break
        jicha_result.extend(info)
        
        endTime = startTime
        
        

    open_interest_hist_df = pd.DataFrame(open_interest_hist_result,
                      columns=['pair', 'contractType', 'sumOpenInterest', 'sumOpenInterestValue', 'timestamp']).sort_values('timestamp')
    open_interest_hist_df['symbol'] = open_interest_hist_df['pair']
    open_interest_hist_df['interval'] = interval
    open_interest_hist_df['datetime'] = open_interest_hist_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    open_interest_hist_df['market_cate1'] = '1000'
    open_interest_hist_df['market_cate2'] = cate2_conf[conf_type]
    open_interest_hist_df = open_interest_hist_df[['market_cate1', 'market_cate2', 'symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    top_long_short_account_ratio_df = pd.DataFrame(top_long_short_account_ratio_result,
                      columns=['pair', 'longShortRatio', 'longAccount', 'shortAccount', 'timestamp']).sort_values('timestamp')
    top_long_short_account_ratio_df['symbol'] = top_long_short_account_ratio_df['pair']
    top_long_short_account_ratio_df['interval'] = interval
    top_long_short_account_ratio_df['long'] = top_long_short_account_ratio_df['longAccount']
    top_long_short_account_ratio_df['short'] = top_long_short_account_ratio_df['shortAccount']
    top_long_short_account_ratio_df['datetime'] = top_long_short_account_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    top_long_short_account_ratio_df['market_cate1'] = '1000'
    top_long_short_account_ratio_df['market_cate2'] = cate2_conf[conf_type]
    top_long_short_account_ratio_df = top_long_short_account_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    top_long_short_position_ratio_df = pd.DataFrame(top_long_short_position_ratio_result,
                      columns=['pair', 'longShortRatio', 'longPosition', 'shortPosition', 'timestamp']).sort_values('timestamp')
    top_long_short_position_ratio_df['symbol'] = top_long_short_position_ratio_df['pair']
    top_long_short_position_ratio_df['interval'] = interval
    top_long_short_position_ratio_df['long'] = top_long_short_position_ratio_df['longPosition']
    top_long_short_position_ratio_df['short'] = top_long_short_position_ratio_df['shortPosition']
    top_long_short_position_ratio_df['datetime'] = top_long_short_position_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    top_long_short_position_ratio_df['market_cate1'] = '1000'
    top_long_short_position_ratio_df['market_cate2'] = cate2_conf[conf_type]
    top_long_short_position_ratio_df = top_long_short_position_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)

    global_long_short_account_ratio_df = pd.DataFrame(global_long_short_account_ratio_result,
                      columns=['pair', 'longShortRatio', 'longAccount', 'shortAccount', 'timestamp']).sort_values('timestamp')
    global_long_short_account_ratio_df['symbol'] = global_long_short_account_ratio_df['pair']
    global_long_short_account_ratio_df['interval'] = interval
    global_long_short_account_ratio_df['long'] = global_long_short_account_ratio_df['longAccount']
    global_long_short_account_ratio_df['short'] = global_long_short_account_ratio_df['shortAccount']
    global_long_short_account_ratio_df['datetime'] = global_long_short_account_ratio_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    global_long_short_account_ratio_df['market_cate1'] = '1000'
    global_long_short_account_ratio_df['market_cate2'] = cate2_conf[conf_type]
    global_long_short_account_ratio_df = global_long_short_account_ratio_df[['market_cate1', 'market_cate2', 'symbol', 'longShortRatio', 'long', 'short', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    taker_buy_sell_vol_df = pd.DataFrame(taker_buy_sell_vol_result,
                      columns=['pair', 'contractType', 'takerBuyVol', 'takerSellVol', 'takerBuyVolValue', 'takerSellVolValue', 'timestamp']).sort_values('timestamp')
    taker_buy_sell_vol_df['symbol'] = taker_buy_sell_vol_df['pair']
    taker_buy_sell_vol_df['interval'] = interval
    taker_buy_sell_vol_df['buySellRatio'] = taker_buy_sell_vol_df['takerBuyVol'].astype(float)/(taker_buy_sell_vol_df['takerSellVol'].astype(float)+1)
    taker_buy_sell_vol_df['buy'] = taker_buy_sell_vol_df['takerBuyVol']
    taker_buy_sell_vol_df['sell'] = taker_buy_sell_vol_df['takerSellVol']
    taker_buy_sell_vol_df['buyValue'] = taker_buy_sell_vol_df['takerBuyVolValue']
    taker_buy_sell_vol_df['sellValue'] = taker_buy_sell_vol_df['takerSellVolValue']
    taker_buy_sell_vol_df['datetime'] = taker_buy_sell_vol_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    taker_buy_sell_vol_df['market_cate1'] = '1000'
    taker_buy_sell_vol_df['market_cate2'] = cate2_conf[conf_type]
    taker_buy_sell_vol_df = taker_buy_sell_vol_df[['market_cate1', 'market_cate2', 'symbol', 'buySellRatio', 'buy', 'sell', 'buyValue', 'sellValue', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    jicha_df = pd.DataFrame(jicha_result,
                      columns=['pair', 'indexPrice', 'contractType', 'basisRate', 'futuresPrice', 'annualizedBasisRate', 'basis', 'timestamp']).sort_values('timestamp')
    jicha_df['symbol'] = jicha_df['pair']
    jicha_df['interval'] = interval
    jicha_df['datetime'] = jicha_df.timestamp.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    jicha_df['market_cate1'] = '1000'
    jicha_df['market_cate2'] = cate2_conf[conf_type]
    jicha_df = jicha_df[['market_cate1', 'market_cate2', 'symbol', 'indexPrice', 'basisRate', 'futuresPrice', 'basis', 'timestamp', 'interval', 'datetime']].reset_index(drop=True)
    
    
    return open_interest_hist_df, top_long_short_account_ratio_df, top_long_short_position_ratio_df, global_long_short_account_ratio_df, taker_buy_sell_vol_df, jicha_df

def download_symbol_historty_funding_rate(symbol, start_str, end_str, conf_type='bi_heyue', maxLimit=1000):
    endTime = convert_ts_str(end_str)
    finalStartTime = convert_ts_str(start_str)
    if finalStartTime == endTime:
        return pd.DataFrame({})
    startTime = endTime
    result = []
    while startTime >= finalStartTime:
        limit = maxLimit
        startTime = endTime - 8 * 60 * 60 * 1000 * limit
        startTime = max(startTime, finalStartTime)
        deltaTime = (endTime - startTime) / 1000
        limit = math.ceil(deltaTime / (8 * 60 * 60))
        if limit <= 0:
            break
        status, info, headers = binance_request(uri=API_CONF[conf_type]['fund_rate'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        endTime = startTime
        result.extend(info)

    df = pd.DataFrame(result,
                      columns=['symbol', 'fundingTime', 'fundingRate']).sort_values('fundingTime')
    df['fundingDate'] = df.fundingTime.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    return df.reset_index(drop=True)

def download_symbol_historty_kline(symbol, interval, start_str, end_str, conf_type='xianhuo', maxLimit=1000):
    endTime = convert_ts_str(end_str)
    finalStartTime = convert_ts_str(start_str)
    if finalStartTime == endTime:
        return pd.DataFrame({})
    startTime = endTime
    result = []
    while startTime >= finalStartTime:
        limit = maxLimit
        if interval == '1m':
            startTime = endTime - 60 * 1000 * limit
        elif interval == '15m':
            startTime = endTime - 15 * 60 * 1000 * limit
        elif interval == '1h':
            startTime = endTime - 60 * 60 * 1000 * limit
        elif interval == '1d':
            startTime = endTime - 24 * 60 * 60 * 1000 * limit

        startTime = max(startTime, finalStartTime)
        deltaTime = (endTime - startTime) / 1000
        if interval == '1m':
            limit = math.ceil(deltaTime / 60)
        elif interval == '15m':
            limit = math.ceil(deltaTime / (15 * 60))
        elif interval == '1h':
            limit = math.ceil(deltaTime / (60 * 60))
        elif interval == '1d':
            limit = math.ceil(deltaTime / (24 * 60 * 60))
        if limit <= 0:
            break
        status, info, headers = binance_request(uri=API_CONF[conf_type]['kline'], host=API_CONF[conf_type]['host'],
                                                header={
                                                    'symbol': str(symbol), 'interval': str(interval), 'startTime': str(startTime),
                                                    'endTime': str(endTime), 'limit': str(limit)
                                                })
        if status != 200 or len(info) == 0:
            break
        endTime = startTime
        result.extend(info)

    df = pd.DataFrame(result,
                      columns=['open_ts', 'open', 'high', 'low', 'close', 'volume', 'close_ts', 'trd_amt', 'trd_cnt',
                               'taker_volume'
                          , 'taker_amt', 'empty']).sort_values('open_ts')
    df['symbol'] = symbol
    df['interval'] = interval
    df['close_ts'] = df['close_ts'] + 1
    df['open_dt'] = df.open_ts.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    df['close_dt'] = df.close_ts.apply(
        lambda x: datetime.datetime.strftime(datetime.datetime.fromtimestamp(x / 1000), '%Y-%m-%d %H:%M'))
    return df[df.columns.difference(['empty'])].reset_index(drop=True)


def get_db_symbols(db_engine):
    sql = """
    select 
    a.*
    from (
      select
      symbol,`interval`,market_cate2,open_ts,close_ts
      from kline
      ) a
    left join 
    (
      select
      symbol,`interval`,market_cate2,max(open_ts) as open_ts
      from kline
      group by symbol,`interval`,market_cate2
      ) b on (a.symbol=b.symbol and a.interval=b.interval and a.market_cate2=b.market_cate2 and a.open_ts=b.open_ts)
    where b.symbol is not null
    """
    return pd.read_sql(sql=sql, con=db_engine)


def insert_symbol(db_engine, df, type):
    df['market_cate1'] = '1000'
    df['market_cate2'] = cate2_conf[type]
    df = df[['close', 'close_dt', 'close_ts', 'high', 'interval',
             'low', 'open', 'open_dt', 'open_ts', 'symbol', 'taker_amt',
             'taker_volume', 'trd_amt', 'trd_cnt', 'volume', 'market_cate1',
             'market_cate2']]
    df.to_sql('kline', db_engine, if_exists='append', index=False)


def mysql_replace_into(table, conn, keys, data_iter):
    from sqlalchemy.dialects.mysql import insert
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    update_stmt = stmt.on_duplicate_key_update( ** dict(zip(stmt.inserted.keys(),
       stmt.inserted.values())))
    conn.execute(update_stmt)

cate2_conf = {'xianhuo': '1001', 'u_heyue': '1002', 'bi_heyue': '1003'}


def insert_or_update_symbol(db_engine, df, type):
    df['market_cate1'] = '1000'
    df['market_cate2'] = cate2_conf[type]
    df = df[['close', 'close_dt', 'close_ts', 'high', 'interval',
             'low', 'open', 'open_dt', 'open_ts', 'symbol', 'taker_amt',
             'taker_volume', 'trd_amt', 'trd_cnt', 'volume', 'market_cate1',
             'market_cate2']]
    df.to_sql('kline', db_engine, if_exists='append', index=False, method=mysql_replace_into)

def insert_or_update_exchange_info(db_engine, exchange_info):
    ll = []
    for k in exchange_info.keys():
        if k not in cate2_conf.keys():
            continue
        exchange_info[k]['market_cate1'] = '1000'
        exchange_info[k]['market_cate2'] = cate2_conf[k]
        ll.append(exchange_info[k])
    df = pd.concat(ll, axis=0)
    df = df[df.columns.difference(['contractStatus'])]
    df.to_sql('binance_exchange_info', db_engine, if_exists='append', index=False, method=mysql_replace_into)

def auto_update_heyue_longshort_data(symbol, interval, exchange_info, type, db_engine, max_days=30):
    sql = """
    select min(ts) as ts
    from (
        select max(timestamp) as ts from openInterestHist where symbol='#symbol' and `interval`='#interval' and market_cate2='#market_cate2'
        union all
        select max(timestamp) as ts from topLongShortAccountRatio where symbol='#symbol' and `interval`='#interval' and market_cate2='#market_cate2'
        union all
        select max(timestamp) as ts from topLongShortPositionRatio where symbol='#symbol' and `interval`='#interval' and market_cate2='#market_cate2'
        union all
        select max(timestamp) as ts from globalLongShortAccountRatio where symbol='#symbol' and `interval`='#interval' and market_cate2='#market_cate2'
        union all
        select max(timestamp) as ts from takerBuySellVolRatio where symbol='#symbol' and `interval`='#interval' and market_cate2='#market_cate2'
    ) t
    """.replace('#symbol', symbol.replace('_PERP', '')).replace('#interval', interval).replace('#market_cate2', cate2_conf[type])
    ts_df = pd.read_sql(sql=sql, con=db_engine)
    latest_ts = ts_df.ts.values[0]
    if ts_df.empty or latest_ts is None:
        days = max_days
    else:
        latest_ts = ts_df.ts.values[0]
        delta_seconds = (math.floor(time.time() * 1000) - latest_ts) / 1000
        days = math.ceil(delta_seconds / 60 / 60 / 24) + 3
    print('update longshort data symbol:{} interval:{} type:{} {} days ago'.format(symbol, interval, type, days))
    df6 = None
    if type=='u_heyue':
        df1, df2, df3, df4, df5 = download_symbol_u_heyue_historty_short_long_ratio(symbol, interval,'{} day ago UTC'.format(days), '0 day ago UTC')
    else:
        df1, df2, df3, df4, df5, df6 = download_symbol_bi_heyue_historty_short_long_ratio(symbol, interval,'{} day ago UTC'.format(days), '0 day ago UTC')
    if df1 is not None:
        df1.to_sql('openInterestHist', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    if df2 is not None:
        df2.to_sql('topLongShortAccountRatio', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    if df3 is not None:
        df3.to_sql('topLongShortPositionRatio', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    if df4 is not None:
        df4.to_sql('globalLongShortAccountRatio', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    if df5 is not None:
        df5.to_sql('takerBuySellVolRatio', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    if df6 is not None:
        df6.to_sql('jicha', db_engine, if_exists='append', index=False, method=mysql_replace_into)
    
def auto_update_data(symbol, interval, exchange_info, type, db_symbols, db_engine, max_days=1000, maxLimit=1000):
    exists_row = db_symbols[(db_symbols['symbol'] == symbol) & (db_symbols['interval'] == interval) & (db_symbols['market_cate2'] == cate2_conf[type])]
    if exists_row.empty:
        symbol_info = exchange_info[type][exchange_info[type]['symbol'] == symbol]
        if type=='u_heyue' or type=='bi_heyue':
            total_trading_days = math.ceil((time.time() * 1000 - symbol_info.onboardDate.values[0]) / 1000 / 3600 / 24)
        else:
            total_trading_days = max_days
        df = download_symbol_historty_kline(symbol, interval, '{} day ago UTC'.format(min(total_trading_days, max_days)),
                                            '0 day ago UTC', conf_type=type,
                                            maxLimit=API_CONF[type]['kline_limit'])
        if df.shape[0] == 0:
            print(symbol, ' is empty')
        else:
            insert_symbol(db_engine, df, type)
            print('download ', symbol)
    else:
        latest_ts = exists_row.close_ts.values[0]

        delta_seconds = (math.floor(time.time() * 1000) - latest_ts) / 1000
        delta_type = ''
        query_start_minutes = '1d'
        delta_minutes = ''
        if interval == '1d':
            delta_minutes = math.ceil(delta_seconds / 60 / 60 / 24)
            delta_type = 'day'
            query_start_minutes = delta_minutes + 1
        elif interval == '1h':
            delta_minutes = math.ceil(delta_seconds / 60 / 60)
            delta_type = 'hour'
            query_start_minutes = delta_minutes + 1
        elif interval == '15m':
            delta_minutes = math.ceil(delta_seconds / 60)
            delta_type = 'minute'
            query_start_minutes = delta_minutes + 15
        incr_df = download_symbol_historty_kline(symbol, interval,
                                                 '{} {} ago UTC'.format(query_start_minutes, delta_type),
                                                 '0 {} ago UTC'.format(delta_type), conf_type=type,
                                                 maxLimit=API_CONF[type]['kline_limit'])
        print('{} incremental download from {} {} ago'.format(symbol, delta_minutes, delta_type))

        # 合并两个集合
        new_incr_df = incr_df[incr_df.close_ts >= latest_ts]
        insert_or_update_symbol(db_engine, new_incr_df, type)
        print('{} incremental update {} periods'.format(symbol, new_incr_df.shape[0]))
