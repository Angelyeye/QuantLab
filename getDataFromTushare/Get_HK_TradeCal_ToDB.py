'''
Created on 2022年3月26日



港股交易日历
接口：hk_tradecal
描述：获取交易日历
限量：单次最大2000
权限：用户积累2000积分才可调取



输入参数

名称	类型	必选	描述
start_date	str	N	开始日期
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易


输出参数

名称	类型	默认显示	描述
cal_date	str	Y	日历日期
is_open	int	Y	是否交易 '0'休市 '1'交易
pretrade_date	str	Y	上一个交易日


接口示例


pro = ts.pro_api()

df = pro.hk_tradecal(start_date='20200101', end_date='20200708')



数据示例

     cal_date     is_open pretrade_date
    0  20200708        1      20200707
    1  20200707        1      20200706
    2  20200706        1      20200703
    3  20200705        0      20200702
    4  20200704        0      20200702
    5  20200703        1      20200702
    6  20200702        1      20200630
    7  20200701        0      20200629
'''
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL
from basis.Init_Env import init_db, init_ts_pro, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 1000000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'hq_hk_trade_cal'


def write_db(df, db_engine):
    res = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'cal_date': DATE,
                           'is_open': NVARCHAR(1),
                           'pretrade_date': DATE})
    return res


@retry(tries=2, delay=61)
def get_data(ts_pro, rows_limit, offset):
    df = ts_pro.hk_tradecal(limit=rows_limit, offset=offset)
    return df


def get_HK_Trade_Cal(db_engine, ts_pro):
    drop_Table(db_engine, prefix)
    get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()

    get_HK_Trade_Cal(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("交易日历更新完毕，请复核是否正确执行！")
