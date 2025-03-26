from StrategyExample import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dateutil.parser import parse
import datetime
import traceback
from pandas import to_datetime
import time
import os
from tqsdk import TqApi, TqAuth, TqKq, TqAccount

def DateTimeDealFromTq(DateTimeFromTq):
    timenowtemp=DateTimeFromTq+8*3600000000000
    timenowbz=to_datetime(timenowtemp,format="%Y/%m/%d %H:%M:%S")
    timenowbz=str(timenowbz)+'.000000000'
    return timenowbz

def DateChangeToIntFromTq(TimeFromTq):
    time1 = TimeFromTq
    time2 = time1.split('.')[0]
    time3 = time2.split(' ')[0]
    time4 = time3.replace('-', '')
    return int(time4)

def TimeChangeToIntFromTq(TimeFromTq):
    time1 = TimeFromTq
    time2 = time1.split('.')[0]
    time3 = time2.split(' ')[-1]
    time4 = time3.replace(':', '')
    return int(time4)

def MCLDataLoad(MCLFilePath, MCLSymbolName, MCLPeriodName, MCLBackBeginTime, MCLBackEndTime):
    MCLDataPath = os.path.join(MCLFilePath, MCLSymbolName + '-' + MCLPeriodName + '.csv')
    MCLData = pd.read_csv(MCLDataPath, sep=',')
    MCLBackBeginTime = max(MCLBackBeginTime, MCLData['date'].iloc[0])
    if MCLBackEndTime == 0 or MCLBackEndTime > MCLData['date'].iloc[-1]:
        MCLBackEndTime = MCLData['date'].iloc[-1]
    NeedDataIndex = MCLData[(MCLData['date'] >= int(MCLBackBeginTime)) & (MCLData['date'] <= int(MCLBackEndTime))].index.tolist()
    MCLData = MCLData.iloc[NeedDataIndex, :]
    MCLData = MCLData.dropna(axis=0, how='any')
    MCLData = MCLData.drop_duplicates(subset=['date', 'time'],keep='last').reset_index(drop=True)
    MCLData.sort_values(by=['date', 'time'], inplace=True)
    MCLData = MCLData.reset_index(drop=True)
    return MCLData

def MCLDIYEasyQuantTradeRun():
    MCLFilePath = 'D:/MCLKLineDataCenter/Data/Future/TQ/Index/Complete'
    MCLResultFilePath = 'C:/MCL简单回测结果中心'
    if not os.path.exists(MCLResultFilePath):
        os.makedirs(MCLResultFilePath)
    MCLStrategyName = 'MCLMA1'
    MCLSymbolName = 'MA'
    MCLPeriodName = '1M'
    MCLSymbolNameNow = 'CZCE.MA109'
    MCLPeriodNameNow = '1'
    MCLBackBeginTime = 20210801
    MCLBackEndTime = 0
    MCLFollowBeginTime = 20200101
    MCLLots = 1
    MCLInitialMoney = 1
    MCLCostType = 0
    MCLMultiplier = 10
    MCLMinMove = 1
    MCLCostMoney = 20
    MCLCostPercent = 0.001
    MCLData = MCLDataLoad(MCLFilePath, MCLSymbolName, MCLPeriodName, MCLBackBeginTime, MCLBackEndTime)
    MCLKLine = MCLData.copy()
    MCLNormalDataFinal = MCLMA1(MCLKLine)
    MCLRealPosiion = MCLNormalDataFinal['Position'].iloc[-1]
    print('策略初始化成功,当前理论仓位为:' + str(MCLRealPosiion) + ',当前策略:' + MCLStrategyName + ',交易品种:' + MCLSymbolNameNow + ',下单份数:' + str(MCLLots))
    TQAuth = '天勤账号' + ',' + '天勤密码'
    api = TqApi(TqKq(), auth=TQAuth)
    klines = api.get_kline_serial(MCLSymbolNameNow, int(MCLPeriodNameNow) * 60, 200)
    Quote = api.get_quote(MCLSymbolNameNow)
    while True:
        try:
            api.wait_update()
            KlinesNowCopy = klines.copy()
            C = list(map(DateTimeDealFromTq, list(KlinesNowCopy['datetime'])))
            KlinesNowCopy['datetime'] = C
            A = list(map(DateChangeToIntFromTq, list(KlinesNowCopy.datetime)))
            B = list(map(TimeChangeToIntFromTq, list(KlinesNowCopy.datetime)))
            KlinesNowCopy['date'] = A
            KlinesNowCopy['time'] = B
            KLinesNeed = KlinesNowCopy.copy()
            KLinesNeed = KLinesNeed[['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'open_oi', 'close_oi']]
            KLinesNeed.columns = ['date', 'time', 'open', 'high', 'low', 'close', 'vol', 'open_oi', 'close_oi']
            MCLData = pd.concat([MCLData, KLinesNeed])
            MCLData = MCLData.dropna(axis=0,how='any')
            MCLData = MCLData.drop_duplicates(subset=['date', 'time'], keep='last').reset_index(drop=True)
            MCLData = MCLData.reset_index(drop=True)
            MCLData.sort_values(by=['date', 'time'],inplace=True)
            MCLData = MCLData.reset_index(drop=True)
            MCLKLine = MCLData.copy()
            MCLNormalDataFinal = MCLMA1(MCLKLine)
            if MCLNormalDataFinal['Position'].iloc[-2] != MCLRealPosiion:
                print('信号仓位变化!准备下单!旧仓位:' + str(MCLRealPosiion) + ',新仓位:' + str(MCLNormalDataFinal['Position'].iloc[-1]))
                bidprice1 = Quote.bid_price1
                askprice1 = Quote.ask_price1
                timetesttemp1 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                timetesttemp2 = timetesttemp1[0:10] + '_' + timetesttemp1[11:].replace(":", "-")
                if MCLNormalDataFinal['Position'].iloc[-2] >= 0 and MCLRealPosiion > MCLNormalDataFinal['Position'].iloc[-1]:
                    TradeLotsNow = abs(MCLRealPosiion - MCLNormalDataFinal['Position'].iloc[-1])
                    order = api.insert_order(symbol=MCLSymbolNameNow, direction="SELL", offset="CLOSE", volume=TradeLotsNow * MCLLots, limit_price=askprice1)
                    print(timetesttemp2 + '-->多头平仓!委托数量:' + str(TradeLotsNow * MCLLots) + ',委托价格:' + str(askprice1))
                elif MCLNormalDataFinal['Position'].iloc[-1] <= 0 and MCLRealPosiion < MCLNormalDataFinal['Position'].iloc[-1]:
                    TradeLotsNow = abs(MCLRealPosiion - MCLNormalDataFinal['Position'].iloc[-1])
                    order = api.insert_order(symbol=MCLSymbolNameNow, direction="BUY", offset="CLOSE", volume=TradeLotsNow * MCLLots, limit_price=bidprice1)
                    print(timetesttemp2 + '-->空头平仓!委托数量:' + str(TradeLotsNow * MCLLots) + ',委托价格:' + str(bidprice1))
                elif MCLRealPosiion >= 0 and MCLNormalDataFinal['Position'].iloc[-1] > MCLRealPosiion:
                    TradeLotsNow = abs(MCLRealPosiion - MCLNormalDataFinal['Position'].iloc[-1])
                    order = api.insert_order(symbol=MCLSymbolNameNow, direction="BUY", offset="OPEN", volume=TradeLotsNow * MCLLots, limit_price=bidprice1)
                    print(timetesttemp2 + '-->多头开仓!委托数量:' + str(TradeLotsNow * MCLLots) + ',委托价格:' + str(bidprice1))
                elif MCLRealPosiion <= 0 and MCLNormalDataFinal['Position'].iloc[-1] < MCLRealPosiion:
                    TradeLotsNow = abs(MCLRealPosiion - MCLNormalDataFinal['Position'].iloc[-1])
                    order = api.insert_order(symbol=MCLSymbolNameNow, direction="SELL", offset="OPEN", volume=TradeLotsNow * MCLLots, limit_price=askprice1)
                    print(timetesttemp2 + '-->空头开仓!委托数量:' + str(TradeLotsNow * MCLLots) + ',委托价格:' + str(askprice1))
                elif MCLRealPosiion > 0 and MCLNormalDataFinal['Position'].iloc[-1] < 0:
                    TradeLotsNow1 = abs(MCLRealPosiion)
                    TradeLotsNow2 = abs(MCLNormalDataFinal['Position'].iloc[-1])
                    order1 = api.insert_order(symbol=MCLSymbolNameNow, direction="SELL", offset="CLOSE", volume=TradeLotsNow1 * MCLLots, limit_price=askprice1)
                    order2 = api.insert_order(symbol=MCLSymbolNameNow, direction="SELL", offset="OPEN", volume=TradeLotsNow2 * MCLLots, limit_price=askprice1)
                    print(timetesttemp2 + '-->多头反手!平仓数量:' + str(TradeLotsNow1 * MCLLots) + ',开仓数量:' + str(TradeLotsNow2 * MCLLots) + ',委托价格:' + str(askprice1))
                elif MCLRealPosiion < 0 and MCLNormalDataFinal['Position'].iloc[-1] > 0:
                    TradeLotsNow1 = abs(MCLRealPosiion)
                    TradeLotsNow2 = abs(MCLNormalDataFinal['Position'].iloc[-1])
                    order1 = api.insert_order(symbol=MCLSymbolNameNow, direction="BUY", offset="CLOSE", volume=TradeLotsNow1 * MCLLots, limit_price=bidprice1)
                    order2 = api.insert_order(symbol=MCLSymbolNameNow, direction="BUY", offset="OPEN", volume=TradeLotsNow2 * MCLLots, limit_price=bidprice1)
                    print(timetesttemp2 + '-->空头反手!平仓数量:' + str(TradeLotsNow1 * MCLLots) + ',开仓数量:' + str(TradeLotsNow2 * MCLLots) + ',委托价格:' + str(bidprice1))
                MCLRealPosiion = MCLNormalDataFinal['Position'].iloc[-1]
                print('重置最新仓位为:' + str(MCLRealPosiion))
        except:
            dferror = traceback.format_exc()
            print(dferror)               

if __name__ == '__main__':
    MCLDIYEasyQuantTradeRun()