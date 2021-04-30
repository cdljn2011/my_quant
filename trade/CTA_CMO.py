'''backtest
start: 2017-01-01 00:00:00
end: 2020-12-01 00:00:00
period: 1h
basePeriod: 1h
exchanges: [{"eid":"Futures_CTP","currency":"FUTURES"}]
'''


# 外部参数
ma_length = 150
cmo_length = 10
cmo_value = 10


# 导入第三方库
import talib
import numpy as np


# 定义全局变量，记录开平仓
mp = 0


# 获取K线列表中的收盘价数据
def get_data(bar):
    arr = []
    for i in bar:
        arr.append(i['Close'])
    return arr


def bar():
    bar = _C(exchange.GetRecords)                 # 获取K线列表
    if len(bar) < ma_length:                      # 如果K线列表长度过小
        return                                    # 就直接返回
    bar.pop()                                     # 删除列表最后一个元素
    price = bar[-1]['Close']                      # 最新价格
    np_arr = np.array(get_data(bar))              # 转换数据类型
    cmo = abs(talib.CMO(np_arr, cmo_length)[-1])  # 计算CMO数据
    ma1 = talib.MA(np_arr, ma_length)[-1]         # 计算长期均线 
    ma2 = talib.MA(np_arr, ma_length / 2)[-1]     # 计算短期均线
    global mp                                     # 引入全局变量
    if mp > 0 and price < ma2:                    # 如果当前持多单，并且价格小于短期均线
        exchange.SetDirection("closebuy")         # 设置交易方向和类型
        exchange.Sell(price - 1, 1)               # 平多单
        mp = 0                                    # 重置虚拟持仓
    if mp < 0 and price > ma2:                    # 如果当前持空单，并且价格大于短期均线
        exchange.SetDirection("closesell")        # 设置交易方向和类型
        exchange.Buy(price, 1)                    # 平空单
        mp = 0                                    # 重置虚拟持仓
    if mp == 0 and abs(cmo) > cmo_value:          # 如果当前无持仓，并且CMO的绝对值大于cmo_value
        if price > ma1 and price > ma2:           # 如果价格大于长期和短期均线
            exchange.SetDirection("buy")          # 设置交易方向和类型
            exchange.Buy(price, 1)                # 开多单
            mp = 1                                # 重置虚拟持仓
        if price < ma1 and price < ma2:           # 如果价格小于长期和短期均线
            exchange.SetDirection("sell")         # 设置交易方向和类型
            exchange.Sell(price - 1, 1)           # 开空单
            mp = -1                               # 重置虚拟持仓


def main():
    _C(exchange.SetContractType, 'c000')          # 订阅行情
    while True:                                   # 进入无线循环模式
        bar()                                     # 重复执行bar函数
        Sleep(1000)                               # 使程序休眠1秒
