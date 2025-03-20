'''
Created on 2021年7月18日



龙虎榜每日明细
接口：top_list
描述：龙虎榜每日交易明细
数据历史： 2005年至今
限量：单次最大10000
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
trade_date	str	Y	交易日期
ts_code	str	N	股票代码
输出参数

名称	类型	默认显示	描述
trade_date	str	Y	交易日期
ts_code	str	Y	TS代码
name	str	Y	名称
close	float	Y	收盘价
pct_change	float	Y	涨跌幅
turnover_rate	float	Y	换手率
amount	float	Y	总成交额
l_sell	float	Y	龙虎榜卖出额
l_buy	float	Y	龙虎榜买入额
l_amount	float	Y	龙虎榜成交额
net_amount	float	Y	龙虎榜净买入额
net_rate	float	Y	龙虎榜净买额占比
amount_rate	float	Y	龙虎榜成交额占比
float_values	float	Y	当日流通市值
reason	str	Y	上榜理由

'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_db, init_ts_pro, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 200  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_toplist'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=10000,
                         dtype={'trade_date': DATE,
                                'ts_code': NVARCHAR(20),
                                'name': NVARCHAR(255),
                                'close': DECIMAL(17, 2),
                                'pct_change': DECIMAL(17, 6),
                                'turnover_rate': DECIMAL(17, 6),
                                'amount': DECIMAL(17, 2),
                                'l_sell': DECIMAL(17, 2),
                                'l_buy': DECIMAL(17, 2),
                                'l_amount': DECIMAL(17, 2),
                                'net_amount': DECIMAL(17, 2),
                                'net_rate': DECIMAL(17, 6),
                                'amount_rate': DECIMAL(17, 6),
                                'float_values': DECIMAL(17, 2),
                                'reason': NVARCHAR(255), })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.top_list(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_TopList(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()
    currentDate = init_currentDate()
    str_date = '20210101'
    end_date = currentDate

    get_TopList(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("龙虎榜股票信息加载完毕，请复核是否正确执行！")
