import pandas as pd
from talib import MA
from talib import KAMA
# ========================================== 隔夜策略模块 ========================================== #
def MCLMA1(MCLKLine):
    #---------------------------Step1:策略参数预设-----------------------#
    MA1Length = 6
    MA2Length = 30
    
    #---------------------------Step2:指标计算-----------------------#
    ma1 = MA(MCLKLine['close'], MA1Length)
    ma2 = MA(MCLKLine['close'], MA2Length)
    #将指标新增进dataframe
    MCLKLine['ma1'] = ma1
    MCLKLine['ma2'] = ma2

    #---------------------------Step3:信号生成-----------------------#
    # 生成为0的成交价格列
    DealPrice = [0] * (MCLKLine.shape[0])
    # 生成为0的仓位列
    Position = [0] * (MCLKLine.shape[0])
    # 全局控制仓位
    MCLPosition = 0
    # 循环方式生成信号
    for j in range(MA2Length - 1, MCLKLine.shape[0]):
        # 仓位初始化
        Position[j] = MCLPosition
        # 策略信号生成
        MCLBuySignal = MCLKLine['ma1'].iloc[j-1] > MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] <= MCLKLine['ma2'].iloc[j-2]
        MCLSellShortSignal = MCLKLine['ma1'].iloc[j-1] < MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] >= MCLKLine['ma2'].iloc[j-2]
        MCLSellSignal = MCLKLine['ma1'].iloc[j-1] < MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] >= MCLKLine['ma2'].iloc[j-2]
        MCLBuyToCoverSignal = MCLKLine['ma1'].iloc[j-1] > MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] <= MCLKLine['ma2'].iloc[j-2]
        # 下单动作
        # 平多
        if (MCLPosition > 0) and MCLSellSignal:
            Position[j] = 0
            MCLPosition = 0
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 平空
        if (MCLPosition < 0) and MCLBuyToCoverSignal:
            Position[j] = 0
            MCLPosition = 0
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 开多
        if (MCLPosition == 0) and MCLBuySignal:
            Position[j] = 1
            MCLPosition = 1
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 开空
        if (MCLPosition == 0) and MCLSellShortSignal:
            Position[j] = -1
            MCLPosition = -1
            DealPrice[j] = MCLKLine['open'].iloc[j]

    MCLKLine['Position'] = Position
    MCLKLine['DealPrice'] = DealPrice
    MCLKLineFinal = MCLKLine.fillna(0)
    #---------------------------Step4:生成标准化返回数据-----------------------#
    # 新建一个信号DataFrame
    MCLNormalDataFinal = MCLKLine
    MCLNormalDataFinal['BeforePosition'] = MCLKLineFinal['Position']
    MCLNormalDataFinal['Position'] = MCLKLineFinal['Position']
    MCLNormalDataFinal['DealPrice'] = MCLKLineFinal['DealPrice']
    return MCLNormalDataFinal

def MCLAMA1(MCLKLine):
    #---------------------------Step1:策略参数预设-----------------------#
    MA1Length = 6
    MA2Length = 30
    
    #---------------------------Step2:指标计算-----------------------#
    ma1 = KAMA(MCLKLine['close'], MA1Length)
    ma2 = KAMA(MCLKLine['close'], MA2Length)
    #将指标新增进dataframe
    MCLKLine['ma1'] = ma1
    MCLKLine['ma2'] = ma2

    #---------------------------Step3:信号生成-----------------------#
    # 生成为0的成交价格列
    DealPrice = [0] * (MCLKLine.shape[0])
    # 生成为0的仓位列
    Position = [0] * (MCLKLine.shape[0])
    # 全局控制仓位
    global MCLPosition
    MCLPosition = 0
    # 循环方式生成信号
    for j in range(MA2Length - 1, MCLKLine.shape[0]):
        # 仓位初始化
        Position[j] = MCLPosition
        # 策略信号生成
        MCLBuySignal = MCLKLine['ma1'].iloc[j-1] > MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] <= MCLKLine['ma2'].iloc[j-2]
        MCLSellShortSignal = MCLKLine['ma1'].iloc[j-1] < MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] >= MCLKLine['ma2'].iloc[j-2]
        MCLSellSignal = MCLKLine['ma1'].iloc[j-1] < MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] >= MCLKLine['ma2'].iloc[j-2]
        MCLBuyToCoverSignal = MCLKLine['ma1'].iloc[j-1] > MCLKLine['ma2'].iloc[j-1] and MCLKLine['ma1'].iloc[j-2] <= MCLKLine['ma2'].iloc[j-2]
        # 下单动作
        # 平多
        if (MCLPosition > 0) and MCLSellSignal:
            Position[j] = 0
            MCLPosition = 0
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 平空
        if (MCLPosition < 0) and MCLBuyToCoverSignal:
            Position[j] = 0
            MCLPosition = 0
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 开多
        if (MCLPosition == 0) and MCLBuySignal:
            Position[j] = 1
            MCLPosition = 1
            DealPrice[j] = MCLKLine['open'].iloc[j]
        # 开空
        if (MCLPosition == 0) and MCLSellShortSignal:
            Position[j] = -1
            MCLPosition = -1
            DealPrice[j] = MCLKLine['open'].iloc[j]

    MCLKLine['Position'] = Position
    MCLKLine['DealPrice'] = DealPrice
    MCLKLineFinal = MCLKLine.fillna(0)
    #---------------------------Step4:生成标准化返回数据-----------------------#
    # 新建一个信号DataFrame
    MCLNormalDataFinal = MCLKLine
    MCLNormalDataFinal['BeforePosition'] = MCLKLineFinal['Position']
    MCLNormalDataFinal['Position'] = MCLKLineFinal['Position']
    MCLNormalDataFinal['DealPrice'] = MCLKLineFinal['DealPrice']
    return MCLNormalDataFinal










