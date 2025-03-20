'''
Created on 2021年7月18日



日线行情
接口：daily
数据说明：交易日每天15点～16点之间。本接口是未复权行情，停牌期间不提供数据。
调取说明：基础积分每分钟内最多调取500次，每次5000条数据，相当于23年历史，用户获得超过5000积分正常调取无频次限制。
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据。

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
pre_close	float	昨收价
change	float	涨跌额
pct_chg	float	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
vol	float	成交量 （手）
amount	float	成交额 （千元）

'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_code

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_stock_daily'


def write_db(df, db_engine):
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
def get_data(ts_pro, code, offset):
    df = ts_pro.daily(ts_code=code, limit=rows_limit, offset=offset)
    return df


def get_Daily_By_Code(db_engine, ts_pro, code):
    print(prefix, '接口：已调用：传入代码： code：', code)
    df = get_and_write_data_by_code(db_engine, ts_pro, code,
                                    get_data, write_db, prefix, times_limit, rows_limit)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_Daily_By_Code(db_engine, ts_pro, '600396.SH')

    print('调用日期：', currentDate)
    end_str = input("当日日线行情加载完毕，请复核是否正确执行！")
