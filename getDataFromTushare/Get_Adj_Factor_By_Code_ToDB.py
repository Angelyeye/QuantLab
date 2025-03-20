'''
复权因子
接口：adj_factor，可以通过数据工具调试和查看数据。
更新时间：早上9点30分
描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
积分要求：2000积分起，5000以上可高频调取

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码
trade_date	str	N	交易日期(YYYYMMDD，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
adj_factor	float	复权因子
接口示例


pro = ts.pro_api()

#提取000001全部复权因子
df = pro.adj_factor(ts_code='000001.SZ', trade_date='')


#提取2018年7月18日复权因子
df = pro.adj_factor(ts_code='', trade_date='20180718')
或者


df = pro.query('adj_factor',  trade_date='20180718')
数据样例

        ts_code trade_date  adj_factor
0     000001.SZ   20180809     108.031
1     000001.SZ   20180808     108.031
2     000001.SZ   20180807     108.031
3     000001.SZ   20180806     108.031
4     000001.SZ   20180803     108.031
5     000001.SZ   20180802     108.031
6     000001.SZ   20180801     108.031
7     000001.SZ   20180731     108.031
8     000001.SZ   20180730     108.031
9     000001.SZ   20180727     108.031
10    000001.SZ   20180726     108.031
11    000001.SZ   20180725     108.031
12    000001.SZ   20180724     108.031
13    000001.SZ   20180723     108.031
14    000001.SZ   20180720     108.031
15    000001.SZ   20180719     108.031
16    000001.SZ   20180718     108.031
17    000001.SZ   20180717     108.031
18    000001.SZ   20180716     108.031
19    000001.SZ   20180713     108.031
20    000001.SZ   20180712     108.031
'''
import datetime
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_code

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 200  # 该接口限制,每分钟最多调用次数
prefix = 'hq_adj_factor'


def write_db(df, db_engine):
    """
    将复权因子数据写入数据库
    
    参数：
        df: 包含复权因子数据的DataFrame，必须包含以下列：
            - ts_code: 股票代码
            - trade_date: 交易日期
            - adj_factor: 复权因子值
        db_engine: 数据库连接引擎
        
    返回：
        int: 成功写入的记录数
        
    关键参数说明：
        chunksize=1000000: 分批写入避免内存溢出
        if_exists='append': 追加模式（需确保表结构已预先创建）
        dtype: 精确匹配数据库字段类型：
            - ts_code: 可变长字符串（支持中文）
            - trade_date: 日期类型
            - adj_factor: 高精度小数（DECIMAL(17,4)保障财务精度）
    """
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'adj_factor': DECIMAL(17, 4)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, code, offset):
    """
    带重试机制的API数据获取函数
    
    参数：
        ts_pro: Tushare Pro API实例
        code: 股票代码（格式：000001.SZ）
        offset: 分页偏移量（每次增加rows_limit实现分页）
    
    返回：
        pd.DataFrame: 包含复权因子数据的二维表格，结构：
            - ts_code: 股票代码
            - trade_date: 交易日期
            - adj_factor: 复权因子值
    
    注意：
        1. 装饰器 @retry 实现失败重试机制：
           - 最大重试次数 tries=2 (共执行3次)
           - 重试间隔 delay=61秒（绕过分钟级API限制）
        2. limit=5000 严格遵循Tushare接口限制
    """
    df = ts_pro.adj_factor(ts_code=code, limit=rows_limit, offset=offset)
    return df


def get_Adj_Factor_By_Code(db_engine, ts_pro, code):
    """
    单支股票复权因子获取入口函数
    
    参数：
        db_engine: 数据库连接引擎
        ts_pro: Tushare Pro API实例
        code: 股票代码（格式：000001.SZ）
    
    功能：
        通过组合工具函数，实现：
        1. 自动分页获取全量历史数据
        2. 遵守API调用频率限制
        3. 数据完整性校验
        4. 异常自动重试
    
    说明：
        最终数据通过 write_db 函数写入数据库，打印日志用于跟踪执行进度
    """
    print(prefix, '接口：已调用：传入代码： code：', code)
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_code(db_engine, ts_pro, code,
                                    get_data,  # 数据获取回调函数
                                    write_db,  # 数据写入回调函数 
                                    prefix,    # 数据库表前缀
                                    times_limit,  # API每分钟最大调用次数
                                    rows_limit)  # 单次请求最大数据量


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_Adj_Factor_By_Code(db_engine, ts_pro, '300431.SZ')

    print('调用日期：', currentDate)
    end_str = input("当日复权因子加载完成，请复核是否正确执行！")
