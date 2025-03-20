'''
Created on 2021年12月5日



期货日线行情
接口：fut_daily，可以通过数据工具调试和查看数据。
描述：期货日线行情数据
限量：单次最大2000条，总量不限制
积分：用户需要至少2000积分才可以调取，未来可能调整积分，请尽量多的积累积分。具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
trade_date	str	N	交易日期
ts_code	str	N	合约代码
exchange	str	N	交易所代码
start_date	str	N	开始日期
end_date	str	N	结束日期
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS合约代码
trade_date	str	Y	交易日期
pre_close	float	Y	昨收盘价
pre_settle	float	Y	昨结算价
open	float	Y	开盘价
high	float	Y	最高价
low	float	Y	最低价
close	float	Y	收盘价
settle	float	Y	结算价
change1	float	Y	涨跌1 收盘价-昨结算价
change2	float	Y	涨跌2 结算价-昨结算价
vol	float	Y	成交量(手)
amount	float	Y	成交金额(万元)
oi	float	Y	持仓量(手)
oi_chg	float	Y	持仓量变化
delv_settle	float	N	交割结算价

'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 2000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_fut_daily'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'pre_close': DECIMAL(17, 2),
                                'pre_settle': DECIMAL(17, 2),
                                'open': DECIMAL(17, 2),
                                'high': DECIMAL(17, 2),
                                'low': DECIMAL(17, 2),
                                'close': DECIMAL(17, 2),
                                'settle': DECIMAL(17, 2),
                                'change1': DECIMAL(17, 2),
                                'vol': DECIMAL(17, 2),
                                'amount': DECIMAL(17, 2),
                                'oi': DECIMAL(17, 2),
                                'oi_chg': DECIMAL(17, 2),
                                'delv_settle': DECIMAL(17, 2), })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.fut_daily(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Ful_Daily(db_engine, ts_pro, start_date, end_date):
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '20211101'
    end_date = currentDate

    get_Ful_Daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日日线行情加载完毕，请复核是否正确执行！")
