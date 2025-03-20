'''
Created on 2021年10月18日



沪深股通十大成交股
接口：hsgt_top10
描述：获取沪股通、深股通每日前十大成交详细数据

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码（二选一）
trade_date	str	N	交易日期（二选一）
start_date	str	N	开始日期
end_date	str	N	结束日期
market_type	str	N	市场类型（1：沪市 3：深市）
输出参数

名称	类型	描述
trade_date	str	交易日期
ts_code	str	股票代码
name	str	股票名称
close	float	收盘价
change	float	涨跌额
rank	int	资金排名
market_type	str	市场类型（1：沪市 3：深市）
amount	float	成交金额（元）
net_amount	float	净成交金额（元）
buy	float	买入金额（元）
sell	float	卖出金额（元）
接口用法

pro = ts.pro_api()

pro.hsgt_top10(trade_date='20180725', market_type='1')
或者


pro.query('hsgt_top10', ts_code='600519.SH', start_date='20180701', end_date='20180725')
数据样例

  trade_date    ts_code  name       close  change  rank  market_type  \
0   20180725  600009.SH  上海机场   62.69    2.0677     9            1
1   20180725  600019.SH  宝钢股份    8.62    0.9368     7            1
2   20180725  600036.SH  招商银行   28.22    1.6937    10            1
3   20180725  600276.SH  恒瑞医药   71.89    1.2393     5            1
4   20180725  600519.SH  贵州茅台  743.81   -0.2133     2            1
5   20180725  600585.SH  海螺水泥   38.23   -0.4427     3            1
6   20180725  600690.SH  青岛海尔   18.09    0.0000     8            1
7   20180725  600887.SH  伊利股份   27.54   -1.7131     6            1
8   20180725  601318.SH  中国平安   62.16    0.6803     1            1
9   20180725  601888.SH  中国国旅   74.19    5.5184     4            1

        amount   net_amount          buy         sell
0  240958518.0   31199144.0  136078831.0  104879687.0
1  245582396.0   81732606.0  163657501.0   81924895.0
2  240655550.0  142328622.0  191492086.0   49163464.0
3  329472455.0  -71519443.0  128976506.0  200495949.0
4  508590993.0  226149667.0  367370330.0  141220663.0
5  357946144.0   51215890.0  204581017.0  153365127.0
6  243840019.0  -55595149.0   94122435.0  149717584.0
7  296552611.0  -40273759.0  128139426.0  168413185.0
8  534002916.0  287838388.0  410920652.0  123082264.0
9  342115066.0  -63262966.0  139426050.0  202689016.0
'''
import datetime
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_start_end_date_and_codelist, get_and_write_data_by_date, drop_Table

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 300  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'hq_hsgt_north_top10'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'trade_date': DATE,
                                'ts_code': NVARCHAR(20),
                                'name': NVARCHAR(20),
                                'close': DECIMAL(17, 4),
                                'change': DECIMAL(17, 4),
                                'rank': DECIMAL(17, 4),
                                'amount': DECIMAL(17, 4),
                                'net_amount': DECIMAL(17, 4),
                                'buy': DECIMAL(17, 4),
                                'sell': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.hsgt_top10(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_hsgt_north_top10(db_engine, ts_pro, start_date, end_date):
    # drop_Table(db_engine, prefix)
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()

    str_date = '20140601'
    end_date = currentDate

    get_hsgt_north_top10(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("加载完成，请复核是否正确执行！")
