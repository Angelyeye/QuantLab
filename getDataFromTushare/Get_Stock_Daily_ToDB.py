'''
2025年3月17日

A股日线行情
接口：daily，可以通过数据工具调试和查看数据
数据说明：交易日每天15点～16点之间入库。本接口是未复权行情，停牌期间不提供数据
调取说明：120积分每分钟内最多调取500次，每次6000条数据，相当于单次提取23年历史
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
trade_date	str	N	交易日期（YYYYMMDD）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
open	float	开盘价
high	float	最高价
low	float	最低价
close	float	收盘价
pre_close	float	昨收价【除权价，前复权】
change	float	涨跌额
pct_chg	float	涨跌幅 【基于除权后的昨收计算的涨跌幅：（今收-除权昨收）/除权昨收 】
vol	float	成交量 （手）
amount	float	成交额 （千元）
接口示例


pro = ts.pro_api()

df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')

#多个股票
df = pro.daily(ts_code='000001.SZ,600000.SH', start_date='20180701', end_date='20180718')
或者


df = pro.query('daily', ts_code='000001.SZ', start_date='20180701', end_date='20180718')
也可以通过日期取历史某一天的全部历史


df = pro.daily(trade_date='20180810')
数据样例

 ts_code     trade_date  open  high   low  close  pre_close  change    pct_chg  vol        amount
0  000001.SZ   20180718  8.75  8.85  8.69   8.70       8.72   -0.02       -0.23   525152.77   460697.377
1  000001.SZ   20180717  8.74  8.75  8.66   8.72       8.73   -0.01       -0.11   375356.33   326396.994
2  000001.SZ   20180716  8.85  8.90  8.69   8.73       8.88   -0.15       -1.69   689845.58   603427.713
3  000001.SZ   20180713  8.92  8.94  8.82   8.88       8.88    0.00        0.00   603378.21   535401.175
4  000001.SZ   20180712  8.60  8.97  8.58   8.88       8.64    0.24        2.78  1140492.31  1008658.828
5  000001.SZ   20180711  8.76  8.83  8.68   8.78       8.98   -0.20       -2.23   851296.70   744765.824
6  000001.SZ   20180710  9.02  9.02  8.89   8.98       9.03   -0.05       -0.55   896862.02   803038.965
7  000001.SZ   20180709  8.69  9.03  8.68   9.03       8.66    0.37        4.27  1409954.60  1255007.609
8  000001.SZ   20180706  8.61  8.78  8.45   8.66       8.60    0.06        0.70   988282.69   852071.526
9  000001.SZ   20180705  8.62  8.73  8.55   8.60       8.61   -0.01       -0.12   835768.77   722169.579

'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date, check_or_create_table

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_stock_daily'
dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'open': DECIMAL(17, 2),
                                'high': DECIMAL(17, 2),
                                'low': DECIMAL(17, 2),
                                'close': DECIMAL(17, 2),
                                'pre_close': DECIMAL(17, 2),
                                'change': DECIMAL(17, 2),
                                'pct_chg': DECIMAL(17, 2),
                                'vol': DECIMAL(17, 2),
                                'amount': DECIMAL(17, 2)}


def write_db(df, db_engine):
    check_or_create_table(db_engine, prefix, dtype)
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'open': DECIMAL(17, 2),
                                'high': DECIMAL(17, 2),
                                'low': DECIMAL(17, 2),
                                'close': DECIMAL(17, 2),
                                'pre_close': DECIMAL(17, 2),
                                'change': DECIMAL(17, 2),
                                'pct_chg': DECIMAL(17, 2),
                                'vol': DECIMAL(17, 2),
                                'amount': DECIMAL(17, 2), })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.daily(start_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Stock_Daily(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_Stock_Daily(db_engine, ts_pro, '19900101', currentDate)

    print('数据日期：', currentDate)
    end_str = input("当日日线行情加载完毕，请复核是否正确执行！")
