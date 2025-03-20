#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-


#这是一个简单的示例，展示了双均线策略的实现。


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import pandas as pd
import backtrader as bt
from basis.Init_Env import init_currentDate, init_db


def get_data_from_tushare2db():
    """将A股票日线数据返回BT Data
    """
    # 参数部分初始化
    currentdate = init_currentDate()
    b_engine = init_db()
    # 策略跟踪的股票代码
    stock_id = "600519.SH"
    # 跟踪启止日期
    start = "20210101"
    end = currentdate
    dt_start = datetime.datetime.strptime(start, "%Y%m%d")
    dt_end = datetime.datetime.strptime(end, "%Y%m%d")

    # 加载数据
    sql = 'select * from hq_stock_daily where ts_code = \'%s\' and trade_date between \'%s\' and \'%s\' ' \
          'order by trade_date ' % (stock_id, start, end)
    df = pd.read_sql_query(sql, b_engine)
    df.sort_values(by=["trade_date"], ascending=True, inplace=True)  # 按日期先后排序
    # 修改、排序、聚合操作后，可能会产生错误，最好做一个reset_index.
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


# 创建策略
class TestStrategy(bt.Strategy):  # 通过继承 Strategy 基类，来构建自己的交易策略子类
    params = (('maperiod', 20),  # 设置最终多少日的均线
              )

    def log(self, txt, dt=None):
        # 策略日志函数
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 引用data[0]数据的收盘价数据
        self.dataclose = self.datas[0].close

        # 用于记录订单状态
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 添加指标：简单移动平均加 sma,第一个参数是行情的line，第二个参数是周期，在params里面定义了
        self.sma = bt.indicators.MovingAverageSimple(self.data0, period=self.params.maperiod)
        self.buy_signal = bt.indicators.CrossOver(self.data.close, self.sma)
        self.sell_signal = bt.indicators.CrossDown(self.data.close, self.sma)


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 提交给代理或者由代理接收的买/卖订单 - 不做操作
            return

        # 检查订单是否执行完毕
        # 注意：如果没有足够资金，代理会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '买入成交, 价格: %.2f, 成交金额: %.2f, 佣金 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # 卖
                self.log('卖出成交, 价格: %.2f, 成交金额: %.2f, 佣金 %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
        # 无等待处理订单
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('权益, 赢亏 %.2f, 总赢亏（含佣金） %.2f' %
                 (trade.pnl, trade.pnlcomm))

    # next 函数在策略中会被逐次调用，依据的是self.datas[0]这个系统时钟
    def next(self):
        # 用日志函数，输出收盘价数据
        self.log('收盘价：%.2f  移动平均价：%.2f' % (self.dataclose[0], self.sma[0]))

        # --------------策略2：MA策略-----------------------
        # 判断是否有交易指令正在进行
        if self.order:
            return
        # 空仓，还未进场，则只能进行买入
        if not self.position:
            if self.buy_signal[0] == 1:
                # 买、买、买
                self.log('买入信号, 收盘价：%.2f > 移动平均价：%.2f' % (self.data0.close[0], self.sma[0]))
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy(sisze=100)
        else:  # 不空仓，考虑卖不卖
            if self.sell_signal[0] == 1:
                # 卖、卖、卖
                self.log('卖出信号, 收盘价：%.2f < 移动平均价：%.2f' % (self.data0.close[0], self.sma[0]))
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell(sisze=100)


if __name__ == '__main__':
    # 创建引擎，实例化Cerebro引擎（Cerebro西班牙语中大脑的意
    cerebro = bt.Cerebro()
    # 添加策略
    cerebro.addstrategy(TestStrategy)
    # 添加数据到引擎
    data = get_data_from_tushare2db()
    cerebro.adddata(data)
    # 设置初始资金
    cerebro.broker.setcash(210000.0)

    # 设置交易单位大小
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    # 设置佣金为千分之一
    cerebro.broker.setcommission(commission=0.0003)
    # 滑点：双边各 0.0001
    cerebro.broker.set_slippage_perc(perc=0.0001)

    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤

    # 打印开始信息
    print('初始价值: %.2f' % cerebro.broker.getvalue())
    # 运行，遍历所有数据
    result = cerebro.run()
    # 打印最后结果
    print('结束价值: %.2f' % cerebro.broker.getvalue())
    print('-----------------------------策略分析结果-----------------------------------')
    # 打印最后结果
    start = result[0]
    # 返回日度收益率序列
    daily_return = pd.Series(start.analyzers.pnl.get_analysis())
    # 打印评价指标
    print("--------------- AnnualReturn -----------------")
    print(start.analyzers._AnnualReturn.get_analysis())
    print("--------------- SharpeRatio -----------------")
    print(start.analyzers._SharpeRatio.get_analysis())
    print("--------------- DrawDown -----------------")
    print(start.analyzers._DrawDown.get_analysis())

    # 可视化回测结果
    plt = cerebro.plot(style='candle')
    plt.show()
