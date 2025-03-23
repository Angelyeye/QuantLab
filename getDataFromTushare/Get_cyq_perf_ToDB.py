'''
每日筹码及胜率
接口：cyq_perf
描述：获取A股每日筹码平均成本和胜率情况，每天17~18点左右更新，数据从2018年开始
来源：Tushare社区
限量：单次最大5000条，可以分页或者循环提取
积分：120积分可以试用(查看数据)，5000积分每天20000次，10000积分每天200000次，15000积分每天不限总量



输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码
trade_date	str	N	交易日期（YYYYMMDD）
start_date	str	N	开始日期
end_date	str	N	结束日期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	股票代码
trade_date	str	Y	交易日期
his_low	float	Y	历史最低价
his_high	float	Y	历史最高价
cost_5pct	float	Y	5分位成本
cost_15pct	float	Y	15分位成本
cost_50pct	float	Y	50分位成本
cost_85pct	float	Y	85分位成本
cost_95pct	float	Y	95分位成本
weight_avg	float	Y	加权平均成本
winner_rate	float	Y	胜率


接口用法


pro = ts.pro_api()

df = pro.cyq_perf(ts_code='600000.SH', start_date='20220101', end_date='20220429')


数据样例

      ts_code trade_date his_low his_high cost_5pct cost_95pct weight_avg winner_rate
0   600000.SH   20220429    0.72    12.16      8.18      11.34       9.76        3.52
1   600000.SH   20220428    0.72    12.16      8.24      11.34       9.76        3.08
2   600000.SH   20220427    0.72    12.16      8.30      11.34       9.76        1.71
3   600000.SH   20220426    0.72    12.16      8.34      11.34       9.76        2.02
4   600000.SH   20220425    0.72    12.16      8.36      11.34       9.77        1.44
..        ...        ...     ...      ...       ...        ...        ...         ...
72  600000.SH   20220110    0.72    12.16      8.60      11.36       9.89        7.62
73  600000.SH   20220107    0.72    12.16      8.60      11.36       9.89        7.59
74  600000.SH   20220106    0.72    12.16      8.60      11.36       9.89        3.92
75  600000.SH   20220105    0.72    12.16      8.60      11.36       9.89        5.65
76  600000.SH   20220104    0.72    12.16      8.60      11.36       9.89        3.93
'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 200  # 该接口限制,每分钟最多调用次数
day_limit = 20000  # 该接口限制,每天最多调用次数
sleeptime = 61
prefix = 'hq_cyq_perf'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'his_low': DECIMAL(17, 4),
                                'his_high': DECIMAL(17, 4),
                                'cost_5pct': DECIMAL(17, 4),
                                'cost_15pct': DECIMAL(17, 4),
                                'cost_50pct': DECIMAL(17, 4),
                                'cost_85pct': DECIMAL(17, 4),
                                'cost_95pct': DECIMAL(17, 4),
                                'weight_avg': DECIMAL(17, 4),
                                'winner_rate': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.cyq_perf(trade_date=idate, limit=rows_limit, offset=offset)
    return df


def get_cyq_perf(db_engine, ts_pro, start_date, end_date):
    # 读取数据，并存储到数据库
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_cyq_perf(db_engine, ts_pro, '20211201', currentDate)

    print('数据日期：', currentDate)
    end_str = input("当日每日筹码及胜率加载完成，请复核是否正确执行！")
