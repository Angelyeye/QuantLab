'''
Created on 2021年7月20日



龙虎榜机构明细
接口：top_inst
描述：龙虎榜机构成交明细
限量：单次最大10000
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
trade_date	str	Y	交易日期
ts_code	str	N	TS代码
输出参数

名称	类型	默认显示	描述
trade_date	str	Y	交易日期
ts_code	str	Y	TS代码
exalter	str	Y	营业部名称
side	str	Y	买卖类型0：买入金额最大的前5名， 1：卖出金额最大的前5名
buy	float	Y	买入额（元）
buy_rate	float	Y	买入占总成交比例
sell	float	Y	卖出额（元）
sell_rate	float	Y	卖出占总成交比例
net_buy	float	Y	净成交额（元）
reason	str	Y	上榜理由

'''
import datetime
import math
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 200  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_topinst'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=100000,
                         dtype={'trade_date': DATE,
                                'ts_code': NVARCHAR(20),
                                'exalter': NVARCHAR(255),
                                'side': DECIMAL(1),
                                'buy': DECIMAL(17, 2),
                                'buy_rate': DECIMAL(17, 2),
                                'sell': DECIMAL(17, 2),
                                'sell_rate': DECIMAL(17, 2),
                                'net_buy': DECIMAL(17, 2),
                                'reason': NVARCHAR(255), })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.top_inst(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_TopInst(db_engine, ts_pro, start_date, end_date):
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    # str_date = currentDate
    str_date = '20220211'
    end_date = '20220211'

    get_TopInst(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("龙虎榜营业部信息加载完毕，请复核是否正确执行！")
