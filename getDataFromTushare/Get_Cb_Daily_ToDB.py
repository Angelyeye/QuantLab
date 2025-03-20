'''
Created on 2021年7月18日



可转债行情
接口：cb_daily
描述：获取可转债行情
限量：单次最大2000条，可多次提取，总量不限制
积分：用户需要至少2000积分才可以调取，5000积分以上频次相对较高，积分越多权限越大，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
ts_code	str	N	TS代码
trade_date	str	N	交易日期(YYYYMMDD格式，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	转债代码
trade_date	str	Y	交易日期
pre_close	float	Y	昨收盘价(元)
open	float	Y	开盘价(元)
high	float	Y	最高价(元)
low	float	Y	最低价(元)
close	float	Y	收盘价(元)
change	float	Y	涨跌(元)
pct_chg	float	Y	涨跌幅(%)
vol	float	Y	成交量(手)
amount	float	Y	成交金额(万元)


接口示例


pro = ts.pro_api()


#获取可转债行情
df = pro.cb_daily(trade_date='20190719')


数据示例

       ts_code trade_date  pre_close     open     high      low    close  \
0    110030.SH   20190719    104.700  104.710  104.960  104.540  104.660
1    113008.SH   20190719    112.600  113.390  113.600  112.800  113.200
2    110031.SH   20190719    107.500  107.500  107.940  107.380  107.520
3    123001.SZ   20190719    114.300  115.500  120.780  114.884  118.879
4    110033.SH   20190719    111.910  111.640  112.500  111.640  112.200
5    110034.SH   20190719    102.360  102.230  102.500  102.230  102.320
6    113009.SH   20190719    107.500  108.000  108.200  107.790  107.800
7    128010.SZ   20190719    100.900  100.900  101.300  100.897  101.000
8    128012.SZ   20190719     97.021   97.013   97.200   97.007   97.029
9    127003.SZ   20190719    101.850  101.850  102.896  101.850  102.399
10   128013.SZ   20190719     96.500   96.307   96.647   96.306   96.500
11   113011.SH   20190719    109.680  109.780  110.990  109.780  110.530
12   113012.SH   20190719    101.330  101.710  103.000  101.590  101.810
13   128014.SZ   20190719     97.000   97.498   97.498   97.103   97.158
14   127004.SZ   20190719     92.252   92.256   92.450   92.256   92.262
15   128015.SZ   20190719     92.799   92.799   93.060   92.790   92.920
16   113013.SH   20190719    113.840  113.860  114.770  113.860  114.060
17   128016.SZ   20190719    114.000  114.125  114.742  112.021  113.800
18   113014.SH   20190719     96.910   96.790   97.230   96.780   96.880
19   128017.SZ   20190719    109.501  109.501  111.880  109.011  109.501
20   113015.SH   20190719    130.070  131.500  132.800  130.000  131.250


     change  pct_chg       vol      amount
0    -0.040  -0.0382    3576.0    374.1486
1     0.600   0.5329    5347.0    605.9335
2     0.020   0.0186      16.0      1.7213
3     4.579   4.0061   85105.8  10134.7401
4     0.290   0.2591    5453.0    611.6870
5    -0.040  -0.0391    3330.0    340.9462
6     0.300   0.2791    9004.0    972.8459
7     0.100   0.0991    2037.3    205.7750
8     0.008   0.0082    4909.5    476.4216
9     0.549   0.5390    3961.0    405.5685
10    0.000   0.0000    6175.4    596.0637
11    0.850   0.7750  140352.0  15524.5109
12    0.480   0.4737    2005.0    204.9667
13    0.158   0.1629     174.0     16.9261
14    0.010   0.0108    3853.0    355.7765
15    0.121   0.1304    9438.1    877.6806
16    0.220   0.1933   19904.0   2278.7127
17   -0.200  -0.1754   21462.9   2434.8231
18   -0.030  -0.0310    1750.0    169.4889
19    0.000   0.0000     364.2     40.3436
20    1.180   0.9072   30730.0   4047.7157
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
prefix = 'hq_cb_daily'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'pre_close': DECIMAL(17, 2),
                                'open': DECIMAL(17, 2),
                                'high': DECIMAL(17, 2),
                                'low': DECIMAL(17, 2),
                                'close': DECIMAL(17, 2),
                                'change': DECIMAL(17, 2),
                                'pct_chg': DECIMAL(17, 2),
                                'vol': DECIMAL(17, 2),
                                'amount': DECIMAL(17, 2)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.cb_daily(start_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Cb_Daily(db_engine, ts_pro, start_date, end_date):
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

    get_Cb_Daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日日线行情加载完毕，请复核是否正确执行！")
