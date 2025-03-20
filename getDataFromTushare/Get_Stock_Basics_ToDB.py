'''
Created on 2021年7月17日


'''
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL
from basis.Init_Env import init_db, init_ts_pro, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 1000000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'hq_stock_basic'


def write_db(df, db_engine):
    res = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'ts_code': NVARCHAR(20),
                           'symbol': NVARCHAR(20),
                           'name': NVARCHAR(255),
                           'area': NVARCHAR(50),
                           'industry': NVARCHAR(50),
                           'market': NVARCHAR(50),
                           'list_date': DATE})
    return res


@retry(tries=2, delay=61)
def get_data(ts_pro, rows_limit, offset):
    df = ts_pro.stock_basic(limit=rows_limit, offset=offset)
    return df


def get_Stock_Basic(db_engine, ts_pro):
    drop_Table(db_engine, prefix)
    get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()

    get_Stock_Basic(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("基础证券信息加载完毕，请复核是否正确执行！")
