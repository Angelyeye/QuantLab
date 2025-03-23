#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# 在文件开头添加以下代码
import sys
from pathlib import Path

# 将项目根目录添加到Python路径
sys.path.append(str(Path(__file__).parent.parent))

# 这是一个KDJ指标超买超卖策略的实现
# KDJ指标是一种常用的技术分析工具，用于判断股票价格走势的超买超卖状态
# K值和D值同时低于20被视为超卖信号（买入机会）
# K值和D值同时高于80被视为超买信号（卖出机会）

import datetime  # 用于日期时间处理
import pandas as pd
import numpy as np
import backtrader as bt
import matplotlib.pyplot as plt  # 用于可视化
from basis.Init_Env import init_currentDate, init_db


def get_data_from_tushare2db():
    """
    从数据库获取A股票日线数据并返回BT Data格式
    
    返回值:
        backtrader.feeds.PandasData: 包含股票历史数据的BT数据对象
    """
    # 参数部分初始化
    currentdate = init_currentDate()
    b_engine = init_db()
    # 策略跟踪的股票代码
    stock_id = "600892.SH"  # 大名城
    # 跟踪启止日期
    start = "20240101"
    end = currentdate
    dt_start = datetime.datetime.strptime(start, "%Y%m%d")
    dt_end = datetime.datetime.strptime(end, "%Y%m%d")

    # 加载数据
    sql = 'select * from hq_stock_daily where ts_code = \'%s\' and trade_date between \'%s\' and \'%s\' ' \
          'order by trade_date ' % (stock_id, start, end)
    df = pd.read_sql_query(sql, b_engine)
    df.sort_values(by=["trade_date"], ascending=True, inplace=True)  # 按日期先后排序
    # 修改、排序、聚合操作后，可能会产生错误，最好做一个reset_index
    # drop=True：把原来的索引index列去掉
    df.reset_index(inplace=True, drop=True)

    # 开始数据清洗：
    # 按日期先后排序
    df.sort_values(by=["trade_date"], ascending=True, inplace=True)
    # 将日期列，设置成index
    df.index = pd.to_datetime(df.trade_date, format='%Y-%m-%d')
    # 增加一列openinterest
    df['openinterest'] = 0.00
    # 取出特定的列
    df = df[['open', 'high', 'low', 'close', 'vol', 'openinterest']]
    # 列名修改成指定的
    df.rename(columns={"vol": "volume"}, inplace=True)

    data = bt.feeds.PandasData(dataname=df, fromdate=dt_start, todate=dt_end)

    return data


# 自定义KDJ指标
class KDJIndicator(bt.Indicator):
    """
    KDJ指标计算类
    
    KDJ指标也称为随机指标，是一种相当新颖、实用的技术分析指标，它起先用于期货市场的分析，后被广泛用于股市的中短期趋势分析。
    
    计算方法：
    1. 计算RSV值：RSV = (C - L9) / (H9 - L9) * 100
       其中，C为收盘价，L9为9日内最低价，H9为9日内最高价
    2. 计算K值：K = 2/3 * K' + 1/3 * RSV
       其中，K'为上一日K值
    3. 计算D值：D = 2/3 * D' + 1/3 * K
       其中，D'为上一日D值
    4. 计算J值：J = 3 * K - 2 * D
    
    参数：
        period (int): RSV计算周期，默认为9
        k_period (int): K值平滑周期，默认为3
        d_period (int): D值平滑周期，默认为3
    
    线条：
        k (line): K值线
        d (line): D值线
        j (line): J值线
    """
    
    # 定义指标的参数
    params = (
        ('period', 9),     # RSV计算周期
        ('k_period', 3),    # K值平滑系数
        ('d_period', 3),    # D值平滑系数
    )
    
    # 定义指标的线条
    lines = ('k', 'd', 'j')
    
    # 定义指标的绘图属性
    plotlines = dict(
        k=dict(color='blue'),
        d=dict(color='red'),
        j=dict(color='green')
    )
    
    def __init__(self):
        # 计算最高价和最低价的滚动窗口
        self.highest_high = bt.indicators.Highest(self.data.high, period=self.p.period)
        self.lowest_low = bt.indicators.Lowest(self.data.low, period=self.p.period)
        
        # 计算RSV值
        self.rsv = 100 * (self.data.close - self.lowest_low) / (self.highest_high - self.lowest_low + 0.00001)  # 加上一个小数避免除零错误
        
        # 计算K、D、J值
        self.lines.k = bt.indicators.EMA(self.rsv, period=self.p.k_period)
        self.lines.d = bt.indicators.EMA(self.lines.k, period=self.p.d_period)
        self.lines.j = 3 * self.lines.k - 2 * self.lines.d


# 创建KDJ策略
class KDJStrategy(bt.Strategy):
    """
    基于KDJ指标的超买超卖交易策略
    
    策略逻辑：
    1. 当K值和D值同时低于20（超卖区域）时，产生买入信号
    2. 当K值和D值同时高于80（超买区域）时，产生卖出信号
    
    参数：
        k_period (int): KDJ指标的K值平滑周期
        d_period (int): KDJ指标的D值平滑周期
        kdj_period (int): KDJ指标的RSV计算周期
        oversold_threshold (int): 超卖阈值，默认为20
        overbought_threshold (int): 超买阈值，默认为80
    """
    
    # 策略参数
    params = (
        ('k_period', 3),                # K值平滑周期
        ('d_period', 3),                # D值平滑周期
        ('kdj_period', 9),              # RSV计算周期
        ('oversold_threshold', 20),     # 超卖阈值
        ('overbought_threshold', 80),   # 超买阈值
    )

    def log(self, txt, dt=None):
        """
        策略日志函数
        
        参数：
            txt (str): 日志文本
            dt (datetime, optional): 日期时间，默认为当前bar的日期
        """
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        """
        策略初始化函数，设置指标和信号
        """
        # 引用data[0]数据的收盘价数据
        self.dataclose = self.datas[0].close

        # 用于记录订单状态
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 添加KDJ指标
        self.kdj = KDJIndicator(
            self.data0,
            period=self.params.kdj_period,
            k_period=self.params.k_period,
            d_period=self.params.d_period
        )
        
        # 定义买入和卖出信号
        # 超卖信号：K值和D值同时低于超卖阈值
        self.buy_signal = bt.And(
            self.kdj.k < self.params.oversold_threshold,
            self.kdj.d < self.params.oversold_threshold
        )
        
        # 超买信号：K值和D值同时高于超买阈值
        self.sell_signal = bt.And(
            self.kdj.k > self.params.overbought_threshold,
            self.kdj.d > self.params.overbought_threshold
        )