# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 17:32:13 2021
策略：币定投
@author: ljn
"""

import ccxt
import pandas as pd
pd.set_option('expand_frame_repr', False) ##True就是可以换行显示。设置成False的时候不允许换行
pd.set_option('precision', 6) ##显示小数点后的位数

# === 参数设置
c_rate = 0.001  # 手续费

# === 读取数据
df = pd.read_csv('btc_1d.csv',skiprows=0) #0-不跳行，否则找不到列标签
print(df)

df = df[['candle_begin_time', 'close']]  # 选取特定的几列

df = df[df['candle_begin_time'] >= '2020-09-27']
df = df[df['candle_begin_time'] <= '2021-04-14']

# === 定投资金计算
df['每次投入资金'] = 1000  # 单次定投1000元买币
df['累计投入资金'] = df['每次投入资金'].cumsum()
df['每次买币数量'] = (df['每次投入资金'] / df['close']) * (1 - c_rate)
#df['每次买币数量'] = (df['每次投入资金'] / df['open']) * (1 - c_rate)
df['累计买币数量'] = df['每次买币数量'].cumsum()
df['平均持有成本'] = df['累计投入资金'] / df['累计买币数量']
df['币市值'] = df['累计买币数量'] * df['close']
#df['币市值'] = df['累计买币数量'] * df['open']


# ===输出数据
print(df[['candle_begin_time', 'close', '累计投入资金', '币市值', '平均持有成本']])
df.to_csv('计算输出数据结果.csv', index=False)


