'''
Created on 2021年10月18日



上市公司公告(信息地雷)
接口：anns
描述：获取上市公司公告数据及原文文本，数据从2000年开始，内容很大，请注意数据调取节奏。
提示：单次最大50行记录，可设置开始和结束时间分阶段获取数据，数据总量不限制
积分：用户需要至少5000积分才可以调取。基础积分有流量控制，积分越多权限越大，请自行提高积分，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码
ann_date	str	N	公告日期
start_date	str	N	公告开始日期
end_date	str	N	公告结束日期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	股票代码
ann_date	str	Y	公告日期
ann_type	str	N	公告类型
title	str	Y	公告标题
content	str	N	公告内容
pub_time	str	N	公告发布时间


接口示例


#获取单个股票公告数据
df = pro.anns(ts_code='002149.SZ', start_date='20190401', end_date='20190509')


#获取最新的50条公告数据
df = pro.anns()


数据示例

         ts_code      ann_date                         title  \
0  002149.SZ  20190424                     2019年第一季度报告全文
1  002149.SZ  20190424                     2019年第一季度报告正文
2  002149.SZ  20190424  关于公司以本公司及控股子公司资产抵押担保的方式申请银行贷款的公告
3  002149.SZ  20190424                    第六届监事会第十五次会议决议
4  002149.SZ  20190424                   第六届董事会第二十二次会议决议
5  002149.SZ  20190424             关于召开2019年第一次临时股东大会的通知
6  002149.SZ  20190423                  2018年度股东大会的法律意见书
7  002149.SZ  20190423                    2018年度股东大会决议公告
8  002149.SZ  20190410                        股票交易异常波动公告
9  002149.SZ  20190404              关于举行2018年度网上业绩说明会的公告


     content
0  西部金属材料股份有限公司2019年第一季度报告全文 \n \n西部金属材料股份有限公司\n ...
1  西部金属材料股份有限公司2019年第一季度报告正文 \n证券代码：002149       ...
2  证券代码：002149        证券简称：西部材料          公告编号：201...
3  证券代码：002149        证券简称：西部材料          公告编号：201...
4  证券代码：002149        证券简称：西部材料          公告编号：201...
5  证券代码：002149            证券简称：西部材料           公告编...
6  西安市高新区绿地中心A座4602-03  邮编710065   \nRoom 4602,A ...
7  证券代码：002149        证券简称：西部材料       公告编号：2019-0...
8  证券代码：002149          证券简称：西部材料         公告编号：20...
9  证券代码：002149            证券简称：西部材料           公告编...
'''
import datetime
import time

import pandas as pd
from retry import retry
from sqlalchemy import TEXT
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate, init_index_codeList, init_stock_codeList
from basis.Tools import get_and_write_data_by_start_end_date_and_codelist

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 10000  # 该接口限制,每分钟最多调用次数
sleeptimes = 61
currentDate = init_currentDate()
prefix = 'hq_alternative_anns'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'ann_date': DATE,
                                'title': NVARCHAR(2048),
                                'content': LONGTEXT})
    return tosqlret


@retry(tries=5, delay=61)
def get_data(ts_pro, code, offset, str_date, end_date):
    df = ts_pro.anns(ts_code=code[0], start_date=str_date, end_date=end_date, limit=rows_limit, offset=offset)
    return df


def get_anns_daily(db_engine, ts_pro, start_date, end_date):
    # drop_table(db_engine)
    codelist = init_stock_codeList(db_engine)
    codelist = codelist[2000:]
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_start_end_date_and_codelist(db_engine, ts_pro, prefix, get_data, write_db, times_limit,
                                                           sleeptimes, rows_limit, codelist, start_date, end_date)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()

    str_date = '19900101'
    end_date = currentDate

    get_anns_daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("加载完成，请复核是否正确执行！")
