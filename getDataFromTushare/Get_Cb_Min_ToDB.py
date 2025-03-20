'''
Created on 2021年10月18日




'''
import datetime
import time

import pandas as pd
from retry import retry
from sqlalchemy import TIME
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

import basis
from basis.Init_Env import init_ts_pro, init_db, init_currentDate, init_ts, init_cb_codeList
from basis.Tools import get_and_write_data_by_code, get_and_write_data_by_start_end_date_and_codelist

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 25000  # 该接口限制,每分钟最多调用次数
sleeptimes = 61
prefix = 'hq_cb_min'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_time': TIME,
                                'open': DECIMAL(17, 4),
                                'close': DECIMAL(17, 4),
                                'high': DECIMAL(17, 4),
                                'low': DECIMAL(17, 4),
                                'vol': DECIMAL(17, 4),
                                'amount': DECIMAL(17, 4),
                                'trade_date': DATE,
                                'pre_close': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, code, offset, str_date_iso, end_date_iso):
    df = ts_pro.pro_bar(code[0], freq='1min',
                        start_date='{} 09:00:00'.format(str_date_iso),
                        end_date='{} 15:01:00'.format(end_date_iso),
                        limit=rows_limit, offset=offset)
    return df


def get_Cb_Min_By_date_and_codelist(db_engine, ts_pro, str_date, end_date):
    codeList = init_cb_codeList(db_engine)
    str_date_iso = datetime.datetime.strptime(str_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_iso = datetime.datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_start_end_date_and_codelist(db_engine, ts_pro, prefix, get_data, write_db, times_limit,
                                                           sleeptimes, rows_limit, codeList, str_date_iso, end_date_iso)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts = init_ts()
    currentDate = init_currentDate()

    str_date = '20210928'
    end_date = '20211231'

    get_Cb_Min_By_date_and_codelist(db_engine, ts, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("加载完成，请复核是否正确执行！")
