# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 21:03:22 2021

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 18:16:49 2021
获取交易对的数据
@author: ljn
"""
import pandas as pd
import datetime
import os
import ccxt
import time

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

time_interval = '1d'  # K线的时间周期，其他可以尝试的值：'1m', '5m', '15m', '30m', '1h', '2h', '1d', '1w', '1M', '1y'，并不是每个交易所都支持
DATA_LIMIT    = 500
OKEX_LIMIT    = 200
 
# =====获取最新数据
def get_history_exchanges_datas(exchange_name, exchange, symbol, start_time, end_time):
    """
    循环获取指定交易所数据的方法.
    :param exchange_name:  交易所名称.
    :param exchange: 交易所对象实例
    :param symbol: 请求的symbol: 交易对： BTC/USDT, ETH/USD等。
    :param start_time: like 2018-1-1
    :param end_time: like 2019-1-1
    :return:
    """  
    
    print(exchange)
    # exit()
    
    #数据存储的位置和名称
    current_path = os.getcwd()
    file_dir = os.path.join(current_path, exchange_name, symbol.replace('/', ''))

    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    #时间计算
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    start_time_stamp = int(time.mktime(start_time.timetuple())) * 1000
    end_time_stamp = int(time.mktime(end_time.timetuple())) * 1000
    print(start_time_stamp)  # 1529233920000
    print(end_time_stamp)

    #每次获取的数据个数
    limit_count = 500
    if exchange_name == 'bitfinex':
        limit_count = DATA_LIMIT
    elif exchange_name == 'bitmex':
        limit_count = DATA_LIMIT
    elif exchange_name == 'binance':
        limit_count = DATA_LIMIT
    elif exchange_name == 'okex':
        limit_count = OKEX_LIMIT
    else:
        limit_count = 200
        
    #循环获取
    while True:
        try:
            print(start_time_stamp)
            data = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_stamp, limit=limit_count)
          
            df = pd.DataFrame(data)
            df.rename(columns={0: 'open_time', 1: 'open', 2: 'high',
                   3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
            start_time_stamp = int(df.iloc[-1]['open_time'])  # 获取下一个次请求的时间.
            
            df['candle_begin_time_real'] = pd.to_datetime(df['open_time'], unit='ms')  # 整理时间
            df['candle_begin_time'] = df['candle_begin_time_real'] + pd.Timedelta(hours=8)  # 北京时间
            df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序

            filename = str(start_time_stamp) + '.csv'
            save_file_path = os.path.join(file_dir, filename)

            print("文件保存路径为：%s" % save_file_path)
            # exit()
            df.set_index('candle_begin_time', drop=True, inplace=True)
            df.to_csv(save_file_path)

            if start_time_stamp > end_time_stamp:
                print("完成数据的请求.")
                break

            time.sleep(3)

        except Exception as error:
            print("运行错误，原因如下：")
            print(error)
            time.sleep(10)


# =====主函数
if __name__ == '__main__':
    # =====设定参数
    symbol = 'BTC/USDT'   # 抓取的品种。btc/usdt现货交易对。其他币种照例修改即可，例如ETH/USDT，LTC/USDT等
    begin_time = '2017-5-1'  #开始时间
    end_time   = '2021-1-1'  #结束时间

    # =====创建ccxt交易所
    name = 'okex'
    exchange = ccxt.okex()   # okex
    #exchange = ccxt.huobipro()  # 火币
    # exchange = ccxt.binance()  # 币安
    
    get_history_exchanges_datas(name, exchange, symbol, begin_time, end_time)
    
    pass
