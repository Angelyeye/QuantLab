# -*- coding: utf-8 -*-

# 该文件主要用于获取交易日历

'''
A股交易日历
接口：trade_cal，可以通过数据工具调试和查看数据。
描述：获取各大交易所交易日历数据,默认提取的是上交所
积分：需2000积分

输入参数

名称	类型	必选	描述
exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源
start_date	str	N	开始日期 （格式：YYYYMMDD 下同）
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易
输出参数

名称	类型	默认显示	描述
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	str	Y	是否交易 0休市 1交易
pretrade_date	str	Y	上一个交易日
接口示例


pro = ts.pro_api()


df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
或者


df = pro.query('trade_cal', start_date='20180101', end_date='20181231')
或者


df = pro.trade_cal() # 不输入参数，获取全量交易日历数据（19901219-20251231）
数据样例

      exchange  cal_date  is_open pretrade_date
0          SSE  20251231        1      20251230
1          SSE  20251230        1      20251229
2          SSE  20251229        1      20251226
3          SSE  20251228        0      20251226
4          SSE  20251227        0      20251226
...        ...       ...      ...           ...
12792      SSE  19901223        0      19901221
12793      SSE  19901222        0      19901221
12794      SSE  19901221        1      19901220
12795      SSE  19901220        1      19901219
12796      SSE  19901219        1          None

[12797 rows x 4 columns]

'''
from retry import retry
from sqlalchemy import DATE, NVARCHAR

from basis.Init_Env import init_db, init_ts_pro, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量（文档没写，测试发现没限制可以全量获取）
offset = 1000000 # 该接口限制每次调用，最大获取数据量（文档没写，测试发现没限制可以全量获取）
times_limit = 1000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'trade_cal'

def write_db(df, db_engine):
    """将交易日历数据写入数据库
    Args:
        df: 包含交易日历数据的DataFrame对象
        db_engine: 已建立的数据库连接引擎
    Returns:
        int: 成功写入的记录数
    """
    res = df.to_sql(
        prefix,          # 数据库表名（由全局变量prefix定义）
        db_engine,       # 数据库连接引擎
        index=False,     # 不保存索引列
        if_exists='append',  # 追加写入模式
        chunksize=10000, # 分块写入提升性能
        dtype={          # 列类型映射
            'exchange': NVARCHAR(20),    # 交易所名称（最大20字符）
            'cal_date': DATE,            # 日期类型
            'is_open': NVARCHAR(1),      # 是否开盘标识（单字符）
            'pretrade_date': DATE        # 前交易日日期
        })
    return res

@retry(tries=2, delay=61)  # 网络请求失败时自动重试（最多2次，间隔61秒）
def get_data(ts_pro, rows_limit, offset):
    """从Tushare Pro接口获取交易日历数据
    Args:
        ts_pro: 已初始化的Tushare Pro接口对象
        rows_limit: 单次请求最大数据行数
        offset: 分页偏移量（翻页步长=rows_limit）
    Returns:
        DataFrame: 包含原始接口数据的表格对象
    """
    df = ts_pro.trade_cal(limit=rows_limit, offset=offset)  # 调用交易日历接口
    return df

def get_Trade_Cal(db_engine, ts_pro):
    """驱动函数：获取并存储完整的交易日历数据
    Args:
        db_engine: 数据库连接引擎
        ts_pro: 初始化后的Tushare Pro接口对象
    """
    # 清空现有数据表（为全量更新做准备）
    drop_Table(db_engine, prefix)
    print('开始获取交易日历数据...')
    
    # 分页获取并写入数据（规避API限制）
    get_and_write_data_by_limit(
        prefix,            # 目标表名
        db_engine,         # 数据库连接
        ts_pro,            # API接口
        get_data,          # 数据获取函数
        write_db,          # 数据写入函数
        rows_limit,        # 单次请求行数限制
        times_limit,       # 每分钟调用次数限制
        sleeptime          # 限流等待时间（秒）
    )


if __name__ == '__main__':
    # 主程序入口
    ts_pro = init_ts_pro()      # 初始化Tushare Pro接口
    db_engine = init_db()       # 初始化数据库连接引擎

    get_Trade_Cal(db_engine, ts_pro)  # 执行交易日历数据获取和存储的主函数

    print('数据日期：', currentDate)  # 输出当前数据日期
    end_str = input("交易日历更新完毕，请复核是否正确执行！")  # 等待用户确认执行结果