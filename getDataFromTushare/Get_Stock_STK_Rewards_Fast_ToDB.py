'''
Created on 2022年3月7日



管理层薪酬和持股
接口：stk_rewards
描述：获取上市公司管理层薪酬和持股
积分：用户需要2000积分才可以调取，5000积分以上频次相对较高，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
ts_code	str	Y	TS股票代码，支持单个或多个代码输入
end_date	str	N	报告期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS股票代码
ann_date	str	Y	公告日期
end_date	str	Y	截止日期
name	str	Y	姓名
title	str	Y	职务
reward	float	Y	报酬
hold_vol	float	Y	持股数


接口用例


pro = ts.pro_api()

#获取单个公司高管全部数据
df = pro.stk_rewards(ts_code='000001.SZ')

#获取多个公司高管全部数据
df = pro.stk_rewards(ts_code='000001.SZ,600000.SH')


数据样例

     ts_code    ann_date  end_date      name     title     reward  hold_vol
0    000001.SZ  20190808  20190630  谢永林       董事长        NaN       0.0
1    000001.SZ  20190808  20190630  胡跃飞     董事,行长        NaN    4104.0
2    000001.SZ  20190808  20190630  陈心颖        董事        NaN       0.0
3    000001.SZ  20190808  20190630   姚波        董事        NaN       0.0
4    000001.SZ  20190808  20190630  叶素兰        董事        NaN       0.0
5    000001.SZ  20190808  20190630  韩小京      独立董事        NaN       0.0
6    000001.SZ  20190808  20190630  蔡方方        董事        NaN       0.0
7    000001.SZ  20190808  20190630   郭建        董事        NaN       0.0
8    000001.SZ  20190808  20190630  郭世邦    董事,副行长        NaN       0.0
9    000001.SZ  20190808  20190630  王春汉      独立董事        NaN       0.0
10   000001.SZ  20190808  20190630  王松奇      独立董事        NaN       0.0
11   000001.SZ  20190808  20190630  郭田勇      独立董事        NaN       0.0
12   000001.SZ  20190808  20190630  杨如生      独立董事        NaN       0.0
13   000001.SZ  20190808  20190630   邱伟  监事长,职工监事        NaN       0.0
14   000001.SZ  20190808  20190630  车国宝      股东监事        NaN       0.0
15   000001.SZ  20190808  20190630  周建国      外部监事        NaN       0.0
16   000001.SZ  20190808  20190630  骆向东      外部监事        NaN       0.0
17   000001.SZ  20190808  20190630  储一昀      外部监事        NaN       0.0
18   000001.SZ  20190808  20190630  孙永桢      职工监事        NaN       0.0
'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate, init_stock_codeList
from basis.Tools import get_and_write_data_by_date, get_and_write_data_by_code_simple, truncate_Table, \
    get_and_write_data_by_long_codelist, check_or_create_table

rows_limit = 4000  # 该接口限制每次调用，最大获取数据量
times_limit = 2000  # 该接口限制,每分钟最多调用次数
sleeptimes = 10  # 暂停多少秒
codes_onetime = 2  # codes_onetime ：一次调用最多获取多少个代码对应的数据
prefix = 'hq_stock_stk_rewards'
dtype={'ts_code': NVARCHAR(20),
    'ann_date': DATE,
    'end_date': DATE,
    'name': NVARCHAR(100),
    'title': NVARCHAR(100),
    'reward': DECIMAL(17, 4),
    'hold_vol': DECIMAL(17, 4)}

def write_db(df, db_engine):
    
    check_or_create_table(db_engine, prefix,dtype)
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'ann_date': DATE,
                                'end_date': DATE,
                                'name': NVARCHAR(100),
                                'title': NVARCHAR(100),
                                'reward': DECIMAL(17, 4),
                                'hold_vol': DECIMAL(17, 4)})
    return tosqlret

@retry(tries=2, delay=61)
def get_data(ts_pro, codes, rows_limit, offset):
    df = ts_pro.stk_rewards(ts_code=codes, limit=rows_limit, offset=offset)
    return df


def get_stock_stk_rewards_fast(db_engine, ts_pro):
    check_or_create_table(db_engine, prefix,dtype)
    truncate_Table(db_engine, prefix)
    codeList = init_stock_codeList(db_engine)
    print(prefix, '接口：开始调用！')
    df = get_and_write_data_by_long_codelist(db_engine, ts_pro, codeList, prefix,
                                        get_data, write_db, codes_onetime,
                                        rows_limit, times_limit, sleeptimes)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_stock_stk_rewards_fast(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("当日数据加载完成，请复核是否正确执行！")
