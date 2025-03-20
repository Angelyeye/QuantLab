'''
Created on 2021年7月18日



每日指标
接口：daily_basic，可以通过数据工具调试和查看数据。
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
积分：用户需要至少600积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
ts_code	str	Y	股票代码（二选一）
trade_date	str	N	交易日期 （二选一）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	TS股票代码
trade_date	str	交易日期
close	float	当日收盘价
turnover_rate	float	换手率（%）
turnover_rate_f	float	换手率（自由流通股）
volume_ratio	float	量比
pe	float	市盈率（总市值/净利润， 亏损的PE为空）
pe_ttm	float	市盈率（TTM，亏损的PE为空）
pb	float	市净率（总市值/净资产）
ps	float	市销率
ps_ttm	float	市销率（TTM）
dv_ratio	float	股息率 （%）
dv_ttm	float	股息率（TTM）（%）
total_share	float	总股本 （万股）
float_share	float	流通股本 （万股）
free_share	float	自由流通股本 （万）
total_mv	float	总市值 （万元）
circ_mv	float	流通市值（万元）

'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_code

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_stock_daily_basic'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'close': DECIMAL(17, 2),
                                'turnover_rate': DECIMAL(17, 2),
                                'turnover_rate_f': DECIMAL(17, 2),
                                'volume_ratio': DECIMAL(17, 2),
                                'pe': DECIMAL(17, 2),
                                'pe_ttm': DECIMAL(17, 2),
                                'pb': DECIMAL(17, 2),
                                'ps': DECIMAL(17, 2),
                                'ps_ttm': DECIMAL(17, 2),
                                'dv_ratio': DECIMAL(17, 2),
                                'dv_ttm': DECIMAL(17, 2),
                                'total_share': DECIMAL(17, 2),
                                'float_share': DECIMAL(17, 2),
                                'free_share': DECIMAL(17, 2),
                                'total_mv': DECIMAL(17, 2),
                                'circ_mv': DECIMAL(17, 2)
                                })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, code, offset):
    df = ts_pro.daily_basic(ts_code=code, limit=rows_limit, offset=offset)
    return df


def get_Daily_Basic_By_Code(db_engine, ts_pro, code):
    print(prefix, '接口：已调用：传入代码： code：', code)
    df = get_and_write_data_by_code(db_engine, ts_pro, code,
                                    get_data, write_db, prefix, times_limit, rows_limit)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = currentDate
    end_date = currentDate

    get_Daily_Basic_By_Code(db_engine, ts_pro, '600396.SH')  # 读取当天的数据到数据库

    print('调用日期：', currentDate)
    end_str = input("当日每日指标加载完成，请复核是否正确执行！")
