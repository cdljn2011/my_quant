import pandas as pd
import tushare as ts
import os

def get_data(code,start='1990-1-1',end='2021-1-1'):
    df=ts.get_k_data(code,autype='qfq',start=start,end=end)  
    df.index=pd.to_datetime(df.date)
    df['ma']=0.0  #Backtrader需要用到
    df['openinterest'] = 0.0  # Backtrader需要用到
    df=df[['open','high','low','close','volume','openinterest',"ma"]]
    
    print(df)
    return df


def acquire_code():   #只下载一只股票数据，且只用CSV保存   未来可以有自己的数据库
    inp_code = "300404"
    inp_start = "1990-1-1"
    inp_end = "2021-3-1"
    df = get_data(inp_code,inp_start,inp_end)
    print(df.info())
    print("—"*30)
    print(df.describe())

    path = os.path.join(os.path.join(os.getcwd(),"数据地址"),inp_code+".csv")
    # path = os.path.join(os.path.join(os.getcwd(),"数据地址"),inp_code+"_30M.csv")
    df.to_csv(path )
  
    
##run
acquire_code()