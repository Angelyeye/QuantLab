'''
Created on 2021年10月18日



场内基金日线行情
接口：fund_daily
描述：获取场内基金日线行情，类似股票日行情
更新：每日收盘后2小时内
限量：单次最大800行记录，总量不限制
积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法

复权行情实现参考：

后复权 = 当日最新价 × 当日复权因子
前复权 = 当日复权价 ÷ 最新复权因子

输入参数

名称	类型	必选	描述
ts_code	str	N	基金代码（二选一）
trade_date	str	N	交易日期（二选一）
start_date	str	N	开始日期
end_date	str	N	结束日期
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
trade_date	str	Y	交易日期
open	float	Y	开盘价(元)
high	float	Y	最高价(元)
low	float	Y	最低价(元)
close	float	Y	收盘价(元)
pre_close	float	Y	昨收盘价(元)
change	float	Y	涨跌额(元)
pct_chg	float	Y	涨跌幅(%)
vol	float	Y	成交量(手)
amount	float	Y	成交额(千元)
'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_code

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 250  # 该接口限制,每分钟最多调用次数
prefix = 'hq_fund_daily'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'pre_close': DECIMAL(17, 4),
                                'open': DECIMAL(17, 4),
                                'high': DECIMAL(17, 4),
                                'low': DECIMAL(17, 4),
                                'close': DECIMAL(17, 4),
                                'change': DECIMAL(17, 4),
                                'pct_chg': DECIMAL(17, 4),
                                'vol': DECIMAL(17, 4),
                                'amount': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, code, offset):
    df = ts_pro.fund_daily(ts_code=code, limit=rows_limit, offset=offset)
    return df


def get_Fund_Daily_By_Code(db_engine, ts_pro, code):
    print(prefix, '接口：已调用：传入代码： code：', code)
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_code(db_engine, ts_pro, code,
                                    get_data, write_db, prefix, times_limit, rows_limit)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = currentDate
    end_date = currentDate

    get_Fund_Daily_By_Code(db_engine, ts_pro, '510050.SH')

    print('数据日期：', currentDate)
    end_str = input("基金每日行情加载完成，请复核是否正确执行！")
