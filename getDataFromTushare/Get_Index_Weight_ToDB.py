'''
Created on 2021年7月18日



指数成分和权重
接口：index_weight
描述：获取各类指数成分和权重，月度数据 。
来源：指数公司网站公开数据
积分：用户需要至少800积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
index_code	str	Y	指数代码 (二选一)
trade_date	str	Y	交易日期 （二选一）
start_date	str	N	开始日期
end_date	None	N	结束日期
输出参数

名称	类型	描述
index_code	str	指数代码
con_code	str	成分代码
trade_date	str	交易日期
weight	float	权重
接口调用


pro = ts.pro_api()

df = pro.index_weight(index_code='399300.SZ', start_date='20180901', end_date='20180930')
数据样例

    index_code   con_code trade_date  weight
0    399300.SZ  000001.SZ   20180903  0.8656
1    399300.SZ  000002.SZ   20180903  1.1330
2    399300.SZ  000060.SZ   20180903  0.1125
3    399300.SZ  000063.SZ   20180903  0.4273
4    399300.SZ  000069.SZ   20180903  0.2010
5    399300.SZ  000157.SZ   20180903  0.1699
6    399300.SZ  000402.SZ   20180903  0.0816
7    399300.SZ  000413.SZ   20180903  0.2023
8    399300.SZ  000415.SZ   20180903  0.0648
9    399300.SZ  000423.SZ   20180903  0.2100
10   399300.SZ  000425.SZ   20180903  0.1884

'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_index_weight'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'index_code': NVARCHAR(20),
                                'con_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'weight': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.index_weight(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Index_Weight(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '19900101'
    end_date = '20220219'

    get_Index_Weight(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日数据加载完毕，请复核是否正确执行！")
