# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 20:12:53 2021

@author: Administrator
"""
import datetime
import os
import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt


# 布林带策略
class Boll_strategy(bt.Strategy):
    #参数
    params = (('size',2500),('periods',20),);
    
    def __init__(self):
        self.dataclose=self.datas[0].close
        ##使用自带的indicators中自带的函数计算出支撑线和压力线，period设置周期，默认是20
        self.lines.top = bt.indicators.BollingerBands(self.datas[0], period=self.params.periods).top;
        self.lines.mid = bt.indicators.BollingerBands(self.datas[0], period=self.params.periods).mid;
        self.lines.bot = bt.indicators.BollingerBands(self.datas[0], period=self.params.periods).bot;
        print(self.lines.top)
        print(self.lines.bot)
        
    def start(self):
        pass

    def prenext(self):
        pass
    
    def next(self):
        if not self.position:
            if self.dataclose <= (self.lines.bot[0])*1:               
                self.order = self.buy(size=self.params.size);
                print(f"{self.data0.datetime.date(0)},买入！价格为{self.data0.close[0]}")      
        else:
            if self.dataclose >= (self.lines.top[0]):
                self.order = self.sell(size=self.params.size);
                print(f"{self.data0.datetime.date(0)},卖出！价格为{self.data0.close[0]}")
                
    def stop(self):
        print(f"stop___{self.datas[0].datetime.date(0)}")
    
#main()函数
if __name__ == '__main__':
    #回测的初始值
    cerebro = bt.Cerebro();

    #绘图显示的参数
    # cerebro.addobserver(bt.observers.Broker);
    cerebro.addobserver(bt.observers.Trades);
    cerebro.addobserver(bt.observers.BuySell);
    # cerebro.addobserver(bt.observers.DrawDown);
    cerebro.addobserver(bt.observers.Value);
    # cerebro.addobserver(bt.observers.TimeReturn);
    
    #加载回测对象
    sharadata = os.path.join(os.path.join(os.getcwd(), "数据地址"), "000063.csv");
    data = pd.read_csv(sharadata, index_col = "date", parse_dates = True);
    calldata = bt.feeds.PandasData( dataname=data,
                                    fromdate=datetime.datetime(2018, 7, 14),
                                    todate=datetime.datetime(2021, 7, 13)
                                  );
    #添加回测数据到BT框架中
    cerebro.adddata(calldata);
    cerebro.broker.set_cash(100000); #回测初始资金
    cerebro.broker.setcommission(commission=0.0025); #交易佣金和印花税
    cerebro.addstrategy(Boll_strategy); #加载策略
    初始资金 = cerebro.broker.getvalue();
    print(f'初始资金:{初始资金}');
    cerebro.run(); #启动策略
    
    #回测结果显示
    期末资金 =  cerebro.broker.getvalue()
    print(f'期末资金:{期末资金}')
    
    cerebro.plot(style='candlestick')
    
   