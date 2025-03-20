'''
场内基金日线行情
接口：fund_daily
描述：获取场内基金日线行情，类似股票日行情，包括ETF行情
更新：每日收盘后2小时内
限量：单次最大2000行记录，总量不限制
积分：用户需要至少2000积分才可以调取，具体请参阅积分获取办法

复权行情实现参考：

后复权 = 当日最新价 × 当日复权因子
前复权 = 当日最新价 ÷ 最新复权因子

输入参数

名称	类型	必选	描述
ts_code	str	N	基金代码
trade_date	str	N	交易日期(YYYYMMDD格式，下同)
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
接口示例


pro = ts.pro_api()

df = pro.fund_daily(ts_code='150018.SZ', start_date='20180101', end_date='20181029')
数据示例

   ts_code     trade_date  pre_close   open   high    low  close  change  \
0    150008.SZ   20181029      1.070  0.964  1.070  0.964  1.070   0.000   
1    150009.SZ   20181029      0.909  0.902  0.917  0.890  0.917   0.008   
2    150012.SZ   20181029      1.073  1.071  1.074  1.071  1.073   0.000   
3    150013.SZ   20181029      1.317  1.340  1.340  1.205  1.299  -0.018   
4    150017.SZ   20181029      1.070  1.044  1.044  1.006  1.009  -0.061   
5    150018.SZ   20181029      0.986  0.986  0.987  0.984  0.986   0.000   
6    150019.SZ   20181029      0.529  0.514  0.514  0.476  0.476  -0.053   
7    150022.SZ   20181029      0.675  0.675  0.675  0.663  0.664  -0.011   
8    150023.SZ   20181029      0.146  0.147  0.147  0.140  0.142  -0.004   
9    150028.SZ   20181029      0.981  0.980  0.981  0.979  0.981   0.000 

     pct_change         vol      amount  
0        0.0000        5.63       0.560  
1        0.8801     1301.00     116.736  
2        0.0000     2914.00     312.377  
3       -1.3667       82.00       9.903  
4       -5.7009     1128.00     114.581  
5        0.0000   893739.59   88075.423  
6      -10.0189  2326318.01  113966.174  
7       -1.6296   102265.00    6823.065  
8       -2.7397   558576.49    7978.773  
9        0.0000      690.88      67.687 
'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 800  # 该接口限制,每分钟最多调用次数
sleeptime = 61
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
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.fund_daily(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Fund_Daily(db_engine, ts_pro, start_date, end_date):
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '20220215'
    end_date = '20220215'

    get_Fund_Daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("基金每日行情加载完成，请复核是否正确执行！")
