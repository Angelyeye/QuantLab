'''
Created on 2021年7月18日



新闻联播
为了更加深入地学习贯彻我党的重要指示精神，利用新时代的新技术弘扬社会主义新价值观，特地整理了过去十年新闻联播的文字稿供大家研究、参考学习。希望大家领悟在心，实务在行，同时也别忘了抓住投资机会。

接口：cctv_news
描述：获取新闻联播文字稿数据，数据开始于2006年6月，超过12年历史
限量：总量不限制
积分：用户积累5000积分可以调取，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
date	str	Y	日期（输入格式：YYYYMMDD 比如：20181211）


输出参数

名称	类型	默认显示	描述
date	str	Y	日期
title	str	Y	标题
content	str	Y	内容


接口调用


pro = ts.pro_api()

df = pro.cctv_news(date='20181211')

'''
import datetime
import math
import time

import pandas as pd
from sqlalchemy import TEXT
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL
from retry import retry
from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 5000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_alternative_cctv_news'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'trade_date': DATE,
                                'title': NVARCHAR(2000),
                                'content': TEXT})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    try:
        df = ts_pro.cctv_news(date=idate, limit=rows_limit, offset=offset)
    except:
        print(prefix, '该日期：', idate, '取不到数据，继续取下一条！')
        return pd.DataFrame()
    df.rename(columns={"date": "trade_date"}, inplace=True)  # 将日期字段修改为trade_date 不然缺失数据补全程序无法识别
    return df


# @retry(tries=100, delay=300)
def get_Alternative_CCTV_News(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    # str_date = '20090627'
    # str_date = '20100104'
    str_date = '19900101'
    end_date = '20060614'

    get_Alternative_CCTV_News(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日数据加载完毕，请复核是否正确执行！")
