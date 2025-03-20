'''
Created on 2022年3月26日



中央结算系统持股明细
接口：ccass_hold_detail
描述：获取中央结算系统机构席位持股明细，数据覆盖全历史
限量：单次最大返回6000条数据，可以循环或分页提取
积分：用户积5000积分可调取



输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码 (e.g. 605009.SH)
hk_code	str	N	港交所代码 （e.g. 95009）
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期


输出参数

名称	类型	默认显示	描述
trade_date	str	Y	交易日期
ts_code	str	Y	股票代号
name	str	Y	股票名称
col_participant_id	str	Y	参与者编号
col_participant_name	str	Y	机构名称
col_shareholding	str	Y	持股量(股)
col_shareholding_percent	str	Y	占已发行股份/权证/单位百分比(%)


接口用法


pro = ts.pro_api()

df = pro.ccass_hold_detail(ts_code='00960.HK', trade_date='20211101', fields='trade_date,ts_code,col_participant_id,col_participant_name,col_shareholding')
数据样例

    trade_date   ts_code col_participant_id       col_participant_name         col_shareholding
0     20211101  00960.HK             B01777         大和资本市场香港有限公司             3000
1     20211101  00960.HK             B01977             中财证券有限公司             3000
2     20211101  00960.HK             B02068             勤丰证券有限公司             3000
3     20211101  00960.HK             B01413       京华山一国际(香港)有限公司             2500
4     20211101  00960.HK             B02120           利弗莫尔证券有限公司             2500
..         ...       ...                ...                  ...              ...
164   20211101  00960.HK             B01459         奕丰证券(香港)有限公司             3000
165   20211101  00960.HK             B01508       西证(香港)证券经纪有限公司             3000
166   20211101  00960.HK             B01511             达利证券有限公司             3000
167   20211101  00960.HK             B01657         日盛嘉富证券国际有限公司             3000
168   20211101  00960.HK             B01712             华生证券有限公司             3000
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
prefix = 'hq_hk_ccass_hold_detail'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'name': NVARCHAR(200),
                                'col_participant_id': NVARCHAR(20),
                                'col_participant_name': NVARCHAR(500),
                                'col_shareholding': NVARCHAR(50),
                                'col_shareholding_percent': NVARCHAR(50)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.ccass_hold_detail(start_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_HK_CCASS_Hold_Detail(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'HK', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '20210101'
    end_date = '20220219'

    get_HK_CCASS_Hold_Detail(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("数据加载完毕，请复核是否正确执行！")
