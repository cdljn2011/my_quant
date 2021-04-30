# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 21:12:57 2021
简单BOLL策略
@author: ljn
"""

import pandas as pd
import os
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


# =====读入数据
symbol = 'BTC-USDT_5m'
df = pd.read_csv('G:\my_quant\my_quant\get_data\okex\BTCUSDT\okex_btc_3moth_1h.csv')
#print(df)

# 任何原始数据读入都进行一下排序、去重，以防万一
df.sort_values(by=['candle_begin_time'], inplace=True)
df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
df.reset_index(inplace=True, drop=True)

# =====转换为其他分钟数据
rule_type = '15T'
df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg({'open': 'first',
     'high': 'max',
     'low': 'min',
     'close': 'last',
     'volume': 'sum',
     'quote_volume': 'sum',
     })

period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
period_df.reset_index(inplace=True)
df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']]
df = df[df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
df.reset_index(inplace=True, drop=True)


