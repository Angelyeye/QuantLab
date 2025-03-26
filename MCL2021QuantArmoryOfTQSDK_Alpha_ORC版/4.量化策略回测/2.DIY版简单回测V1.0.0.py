from StrategyExample import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dateutil.parser import parse
import datetime
import time
import os

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

def MCLRealYear(DateSeries):
    temp1 = datetime.datetime(int(str(DateSeries.iloc[-1])[0:4]), int(str(DateSeries.iloc[-1])[4:6]), int(str(DateSeries.iloc[-1])[6:8]))
    temp2 = datetime.datetime(int(str(DateSeries.iloc[0])[0:4]), int(str(DateSeries.iloc[0])[4:6]), int(str(DateSeries.iloc[0])[6:8]))
    MCLRealYearsNow = round((temp1 - temp2).days / 365, 4)
    return MCLRealYearsNow

def MCLPlotResult(pathnow, MCLPlotName, TimeAll, ReturnCum, ReturnWorth, MaxDfDrawValueNow, MaxDfDrawRatioNow, MCLInitialMoney, MCLFinalReturnValueAll, MCLRealYears, MCLYearReturnValue, MCLBestPercent, MCLBestValue, MCLMaxDrawValueAll, MCLFinalDrawValue, MCLMaxDrawRatioAll, MCLFinalDrawRatio, MCLFinalWorth, MCLYearReturnRatio):
    TimeAllNormal = TimeAll.copy()
    TimeAllNormalFinal = [parse(str(TimeAllNormal[i])) for i in range(0,len(TimeAllNormal))]
    plt.rcParams['font.sans-serif']=['SimHei']                           
    plt.rcParams['axes.unicode_minus']=False                             
    plt.figure(figsize=(12,8))
    ax1 = plt.subplot(211)
    plt.plot(TimeAllNormalFinal, ReturnCum, c='red',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.ylabel('资   产',size=15)
    plt.xlim([TimeAllNormalFinal[0],TimeAllNormalFinal[-1]]) # 日期上下限
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.title(MCLPlotName + '资产曲线,初始资金'+str(MCLInitialMoney)+'万,最后利润为:'+str(round(MCLFinalReturnValueAll,2))+',交易年数:'+str(MCLRealYears)+'\n'+'年化收益金额为:'+str(round(MCLYearReturnValue,2))+'元,收益回撤比(%):'+str(MCLBestPercent)+',收益回撤比(金额):'+str(MCLBestValue),size=15) 
    ax2=plt.subplot(212)
    plt.plot(TimeAllNormalFinal, MaxDfDrawValueNow, c='blue',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.xlabel('日   期',size=15)
    plt.ylabel('回   撤(金额)',size=15)
    plt.title(MCLPlotName+'金额回撤,最大回撤:'+str(round(MCLMaxDrawValueAll,2))+'元'+',最后回撤金额:'+str(round(MCLFinalDrawValue,2))+'元',size=15) 
    plt.xlim([TimeAllNormalFinal[0],TimeAllNormalFinal[-1]]) # 日期上下限
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.tight_layout()
    pathpicnow = os.path.join(pathnow,'1.' + MCLPlotName + '_资产曲线And金额回撤图.jpg')
    plt.savefig(pathpicnow)

    plt.figure(figsize=(12,8))
    ax1=plt.subplot(211)
    plt.plot(TimeAllNormalFinal, ReturnCum, c='red',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.ylabel('资   产',size=15)
    plt.xlim([TimeAllNormalFinal[0], TimeAllNormalFinal[-1]]) # 日期上下限
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.title(MCLPlotName + '资产曲线,初始资金'+str(MCLInitialMoney)+'万,最后利润为:'+str(round(MCLFinalReturnValueAll,2))+',交易年数:'+str(MCLRealYears)+'\n'+'年化收益金额为:'+str(round(MCLYearReturnValue,2))+'元,收益回撤比(%):'+str(MCLBestPercent)+',收益回撤比(金额):'+str(MCLBestValue),size=15) 
    ax2=plt.subplot(212)
    plt.plot(TimeAllNormalFinal, MaxDfDrawRatioNow, c='blue',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.xlabel('日   期',size=15)
    plt.ylabel('回   撤(%)',size=15)
    plt.title(MCLPlotName+'百分比回撤,最大回撤比率:'+str(round(MCLMaxDrawRatioAll,2))+'%'+',最后回撤比例:'+str(round(MCLFinalDrawRatio,2))+'%',size=15) 
    plt.xlim([TimeAllNormalFinal[0], TimeAllNormalFinal[-1]]) # 日期上下限
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.tight_layout()
    pathpicnow = os.path.join(pathnow, '2.' + MCLPlotName +'_资产曲线And百分比回撤图.jpg')
    plt.savefig(pathpicnow)

    plt.figure(figsize=(12,8))
    ax1=plt.subplot(211)
    plt.plot(TimeAllNormalFinal, ReturnWorth, c='red',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.ylabel('净   值',size=15)
    plt.xlim([TimeAllNormalFinal[0], TimeAllNormalFinal[-1]]) # 日期上下限
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.title(MCLPlotName + '净值曲线,初始资金'+str(MCLInitialMoney)+'万,最后净值为:'+str(round(MCLFinalWorth,2))+',交易年数:'+str(MCLRealYears)+'\n'+'年化收益率为:'+str(round(MCLYearReturnRatio,2))+'%,收益回撤比(%):'+str(MCLBestPercent)+',收益回撤比(金额):'+str(MCLBestValue),size=15) 
    ax2=plt.subplot(212)
    plt.plot(TimeAllNormalFinal, MaxDfDrawValueNow, c='blue',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.xlabel('日   期',size=15)
    plt.ylabel('回   撤(金额)',size=15)
    plt.title(MCLPlotName + '金额回撤,最大回撤:'+str(round(MCLMaxDrawValueAll,2))+'元'+',最后回撤金额:'+str(round(MCLFinalDrawValue,2))+'元',size=15) 
    plt.xlim([TimeAllNormalFinal[0], TimeAllNormalFinal[-1]]) # 日期上下限
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.tight_layout()
    pathpicnow = os.path.join(pathnow, '3.' + MCLPlotName + '_净值曲线And金额回撤图.jpg')
    plt.savefig(pathpicnow)

    plt.figure(figsize=(12,8))
    ax1=plt.subplot(211)
    plt.plot(TimeAllNormalFinal, ReturnWorth, c='red',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.ylabel('净   值',size=15)
    plt.xlim([TimeAllNormalFinal[0],TimeAllNormalFinal[-1]]) # 日期上下限
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.title(MCLPlotName + '净值曲线,初始资金'+str(MCLInitialMoney)+'万,最后净值为:'+str(round(MCLFinalWorth,2))+',交易年数:'+str(MCLRealYears)+'\n'+'年化收益率为:'+str(round(MCLYearReturnRatio,2))+'%,收益回撤比(%):'+str(MCLBestPercent)+',收益回撤比(金额):'+str(MCLBestValue),size=15) 
    ax2=plt.subplot(212)
    plt.plot(TimeAllNormalFinal, MaxDfDrawRatioNow, c='blue',lw = 2)
    plt.xticks(color='blue',rotation=90)
    plt.grid(True)
    plt.axis('tight')
    plt.xlabel('日   期',size=15)
    plt.ylabel('回   撤(%)',size=15)
    plt.title(MCLPlotName + '百分比回撤,最大回撤比率:'+str(round(MCLMaxDrawRatioAll,2))+'%'+',最后回撤比例:'+str(round(MCLFinalDrawRatio,2))+'%',size=15) 
    plt.xlim([TimeAllNormalFinal[0], TimeAllNormalFinal[-1]]) # 日期上下限
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))    
    plt.tight_layout()
    pathpicnow = os.path.join(pathnow, '4.' + MCLPlotName + '_净值曲线And百分比回撤图.jpg')
    plt.savefig(pathpicnow)
    return 0 

def MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove):
    if MCLCostType == 0:
        DealCostNow = MCLCostMoney * MCLLots * DealVolumeNow
    else:
        DealCostNow = DealPriceNow * MCLCostPercent * MCLMultiplier * MCLLots * DealVolumeNow
    if DealDirectionNow == '开多' or DealDirectionNow == '开空':
        if MCLOverReturnDataTable.shape[0] != 0:
            if DealFinalNow['Position'].iloc[i-1] != 0 and MCLOverReturnDataTable['当前持仓'].iloc[-1] != 0:
                Temp = ((abs(DealFinalNow['Position'].iloc[i-1]) * DealFinalNow['DealPrice'].iloc[i - 1]) + DealVolumeNow * DealFinalNow['DealPrice'].iloc[i])/ abs(DealFinalNow['Position'].iloc[i])
                DealFinalNow['DealPrice'].iloc[i] = round(Temp / MCLMinMove, 0) * MCLMinMove
        DealReturnNow = -DealCostNow
        DealPosNow = DealFinalNow['Position'].iloc[i]
    elif DealDirectionNow == '平多':
        DealReturnNow =(DealPriceNow - DealFinalNow['DealPrice'].iloc[i-1]) * MCLMultiplier * MCLLots * DealVolumeNow - DealCostNow
        DealPosNow = max(0, DealFinalNow['Position'].iloc[i])
    elif DealDirectionNow == '强制平多':
        DealReturnNow = (DealPriceNow - DealFinalNow['DealPrice'].iloc[i]) * MCLMultiplier * MCLLots * DealVolumeNow - DealCostNow
        DealPosNow = 0
    elif DealDirectionNow == '平空':
        DealReturnNow =(DealFinalNow['DealPrice'].iloc[i-1] - DealPriceNow) * MCLMultiplier * MCLLots * DealVolumeNow - DealCostNow
        DealPosNow = min(0, DealFinalNow['Position'].iloc[i])
    elif DealDirectionNow == '强制平空':
        DealReturnNow = (DealFinalNow['DealPrice'].iloc[i] - DealPriceNow) * MCLMultiplier * MCLLots * DealVolumeNow - DealCostNow
        DealPosNow = 0
    DealIdNow = MCLOverReturnDataTable.shape[0] + 1            
    DealAvgPriceNow = DealFinalNow['DealPrice'].iloc[i]
    ListNeedNow = []
    ListNeedNow.append(DealDate)
    ListNeedNow.append(DealDateTime)
    ListNeedNow.append(DealDirectionNow)
    ListNeedNow.append(DealPriceNow)
    ListNeedNow.append(DealCostNow)
    ListNeedNow.append(DealReturnNow)
    ListNeedNow.append(DealIdNow)
    ListNeedNow.append(DealVolumeNow)
    ListNeedNow.append(DealPosNow)
    ListNeedNow.append(DealAvgPriceNow)
    MCLOverReturnDataTable.loc[MCLOverReturnDataTable.shape[0]] = ListNeedNow
    return DealFinalNow, MCLOverReturnDataTable

def MCLEasyQuantBackTestCoverCount(MCLResultFilePath, MCLData, MCLNormalDataFinal, MCLStrategyName, MCLSymbolName, MCLPeriodName, MCLLots, MCLInitialMoney, MCLCostType, MCLMultiplier, MCLMinMove, MCLCostMoney, MCLCostPercent):
    DealFinalNow = MCLNormalDataFinal.copy()
    NeedDataIndex = DealFinalNow[DealFinalNow['DealPrice'] != 0].index.tolist()
    DealFinalNow = DealFinalNow.iloc[NeedDataIndex, :]
    DealFinalNow = DealFinalNow.reset_index(drop=True)
    MCLOverReturnDataTable = pd.DataFrame(columns=['日期', '时间', '交易方向', '成交价格', '手续费', '本次盈亏', '本次ID', '成交数量', '当前持仓', '平均成本'])
    for i in range(0, DealFinalNow.shape[0]):
        ListNeedNow = []
        DealDate = DealFinalNow['date'].iloc[i]
        DealDateTime = DealFinalNow['time'].iloc[i]
        if i == 0:
            if DealFinalNow['Position'].iloc[i] == 0:
                continue
            if DealFinalNow['Position'].iloc[i] > 0:
                DealDirectionNow = '开多'
            elif DealFinalNow['Position'].iloc[i] < 0:
                DealDirectionNow = '开空'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)
            continue
        if DealFinalNow['Position'].iloc[i-1] >= 0 and DealFinalNow['Position'].iloc[i] > DealFinalNow['Position'].iloc[i-1]:
            DealDirectionNow = '开多'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow =  abs(DealFinalNow['Position'].iloc[i] - DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)

        if DealFinalNow['Position'].iloc[i-1] <= 0 and DealFinalNow['Position'].iloc[i] < DealFinalNow['Position'].iloc[i-1]:
            DealDirectionNow = '开空'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow =  abs(DealFinalNow['Position'].iloc[i] - DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)

        if DealFinalNow['Position'].iloc[i-1] > 0 and DealFinalNow['Position'].iloc[i] >= 0 and DealFinalNow['Position'].iloc[i] < DealFinalNow['Position'].iloc[i-1]:
            DealDirectionNow = '平多'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i] - DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)
        
        if DealFinalNow['Position'].iloc[i-1] < 0 and DealFinalNow['Position'].iloc[i] <= 0 and DealFinalNow['Position'].iloc[i] > DealFinalNow['Position'].iloc[i-1]:
            DealDirectionNow = '平空'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i] - DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)

        if DealFinalNow['Position'].iloc[i-1] < 0 and DealFinalNow['Position'].iloc[i] > 0:
            DealDirectionNow = '平空'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)

            DealDirectionNow = '开多'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)
        
        if DealFinalNow['Position'].iloc[i-1] > 0 and DealFinalNow['Position'].iloc[i] < 0:
            DealDirectionNow = '平多'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i-1])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)

            DealDirectionNow = '开空'
            DealPriceNow = DealFinalNow['DealPrice'].iloc[i]
            DealVolumeNow = abs(DealFinalNow['Position'].iloc[i])
            DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)
        
        if i == DealFinalNow.shape[0] - 1 and DealFinalNow['Position'].iloc[i] != 0:
            DealDate = MCLNormalDataFinal['date'].iloc[-1]
            DealDateTime = MCLNormalDataFinal['time'].iloc[-1]
            if DealFinalNow['Position'].iloc[i] > 0:
                DealDirectionNow = '强制平多'
                DealPriceNow = MCLData['close'].iloc[-1]
                DealVolumeNow = abs(DealFinalNow['Position'].iloc[i])
                DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)      
            elif DealFinalNow['Position'].iloc[i] < 0:
                DealDirectionNow = '强制平空'
                DealPriceNow = MCLData['close'].iloc[-1]
                DealVolumeNow = abs(DealFinalNow['Position'].iloc[i])
                DealFinalNow, MCLOverReturnDataTable = MCLAppendTradeToTable(i, DealFinalNow, MCLOverReturnDataTable, DealDate, DealDateTime, DealDirectionNow, DealPriceNow, DealVolumeNow, MCLCostType, MCLCostMoney, MCLCostPercent, MCLMultiplier, MCLLots, MCLMinMove)
    MCLOverReturnDataTable['累计盈亏'] = MCLOverReturnDataTable['本次盈亏'].cumsum()
    pathnow = os.path.join(MCLResultFilePath, '策略独立结果中心', MCLStrategyName + '-' + MCLSymbolName + '-' + MCLPeriodName)
    if not os.path.exists(pathnow):
        os.makedirs(pathnow)
    pathcsvnow = os.path.join(pathnow, '成交记录.csv')
    MCLOverReturnDataTable.to_csv(pathcsvnow, index=0)
    ReturnDf = MCLOverReturnDataTable['累计盈亏'].copy().astype(float)
    ReturnDf = ReturnDf + MCLInitialMoney * 10000
    ReturnCum = ReturnDf.copy()
    MCLFinalReturnRatioAll = (ReturnDf / (MCLInitialMoney * 10000) - 1) * 100
    ReturnWorth = MCLFinalReturnRatioAll / 100 + 1
    MCLFinalWorth = ReturnWorth.iloc[-1]
    MCLFinalReturnRatioAll = MCLFinalReturnRatioAll.iloc[-1]
    MCLFinalReturnValueAll = ReturnDf - (MCLInitialMoney * 10000)
    MCLFinalReturnValueAll = MCLFinalReturnValueAll.iloc[-1]
    MaxDfNow = ReturnDf.expanding().max()
    MaxDfDrawRatioNow = (round(ReturnDf/MaxDfNow, 4) - 1) * 100
    MaxDfDrawValueNow = ReturnDf - MaxDfNow
    TimeAll = MCLOverReturnDataTable['日期']
    MCLRealYears = MCLRealYear(MCLNormalDataFinal['date'])  
    MCLMaxDrawRatioAll = MaxDfDrawRatioNow.min(axis=0)  
    MCLMaxDrawValueAll = MaxDfDrawValueNow.min(axis=0)  
    MCLFinalDrawValue = MaxDfDrawValueNow.iloc[-1]  
    MCLFinalDrawRatio = MaxDfDrawRatioNow.iloc[-1]  
    MCLYearReturnRatio = round(MCLFinalReturnRatioAll/MCLRealYears, 4)  
    MCLYearReturnValue = round(MCLFinalReturnValueAll/MCLRealYears, 4)  
    MCLBestPercent = round(MCLYearReturnRatio/abs(MCLMaxDrawRatioAll), 4)  
    MCLBestValue = round(MCLYearReturnValue/abs(MCLMaxDrawValueAll), 4)  
    MCLPlotName = MCLStrategyName + '-' + MCLSymbolName + '-' + MCLPeriodName + '平仓盈亏'
    MCLPlotResult(pathnow, MCLPlotName, TimeAll, ReturnCum, ReturnWorth, MaxDfDrawValueNow, MaxDfDrawRatioNow, MCLInitialMoney, MCLFinalReturnValueAll, MCLRealYears, MCLYearReturnValue, MCLBestPercent, MCLBestValue, MCLMaxDrawValueAll, MCLFinalDrawValue, MCLMaxDrawRatioAll, MCLFinalDrawRatio, MCLFinalWorth, MCLYearReturnRatio)
    return 0

def MCLDIYEasyQuantRun():
    time_start = time.time()
    MCLFilePath = 'D:/MCLKLineDataCenter/Data/Future/TQ/Index/Complete'
    MCLResultFilePath = 'C:/MCL简单回测结果中心'
    if not os.path.exists(MCLResultFilePath):
        os.makedirs(MCLResultFilePath)
    MCLStrategyName = 'MCLMA1'
    MCLSymbolName = 'RB'
    MCLPeriodName = '60M'
    MCLBackBeginTime = 20180101
    MCLBackEndTime = 0
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
    MCLEasyQuantBackTestCoverCount(MCLResultFilePath, MCLData, MCLNormalDataFinal, MCLStrategyName, MCLSymbolName, MCLPeriodName, MCLLots, MCLInitialMoney, MCLCostType, MCLMultiplier, MCLMinMove, MCLCostMoney, MCLCostPercent)
    time_end = time.time()
    print('回测成功结束!总耗时:' + str(time_end - time_start) + '秒!')

if __name__ == '__main__':
    MCLDIYEasyQuantRun()