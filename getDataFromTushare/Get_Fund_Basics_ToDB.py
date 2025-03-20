'''
Created on 2021年10月18日



公募基金列表
接口：fund_basic，可以通过数据工具调试和查看数据。
描述：获取公募基金数据列表，包括场内和场外基金
积分：用户需要2000积分才可以调取，单次最大可以提取15000条数据，5000积分以上权限更高，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
market	str	N	交易市场: E场内 O场外（默认E）
status	str	N	存续状态 D摘牌 I发行 L上市中
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	基金代码
name	str	Y	简称
management	str	Y	管理人
custodian	str	Y	托管人
fund_type	str	Y	投资类型
found_date	str	Y	成立日期
due_date	str	Y	到期日期
list_date	str	Y	上市时间
issue_date	str	Y	发行日期
delist_date	str	Y	退市日期
issue_amount	float	Y	发行份额(亿)
m_fee	float	Y	管理费
c_fee	float	Y	托管费
duration_year	float	Y	存续期
p_value	float	Y	面值
min_amount	float	Y	起点金额(万元)
exp_return	float	Y	预期收益率
benchmark	str	Y	业绩比较基准
status	str	Y	存续状态D摘牌 I发行 L已上市
invest_type	str	Y	投资风格
type	str	Y	基金类型
trustee	str	Y	受托人
purc_startdate	str	Y	日常申购起始日
redm_startdate	str	Y	日常赎回起始日
market	str	Y	E场内O场外
'''
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 15000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptimes = 61
currentDate = init_currentDate()
prefix = 'hq_fund_basic'


@retry(tries=2, delay=61)
def get_data(ts_pro, rows_limit, offset):
    df = ts_pro.fund_basic(limit=rows_limit, offset=offset)
    return df


def write_db(df, db_engine):
    res = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=100000,
                    dtype={'ts_code': NVARCHAR(20),
                           'name': NVARCHAR(255),
                           'management': NVARCHAR(255),
                           'custodian': NVARCHAR(255),
                           'fund_type': NVARCHAR(20),
                           'found_date': DATE,
                           'due_date': DATE,
                           'list_date': DATE,
                           'issue_date': DATE,
                           'delist_date': DATE,
                           'issue_amount': DECIMAL(17, 4),
                           'm_fee': DECIMAL(17, 4),
                           'c_fee': DECIMAL(17, 4),
                           'duration_year': DECIMAL(17, 4),
                           'p_value': DECIMAL(17, 4),
                           'min_amount': DECIMAL(17, 4),
                           'exp_return': DECIMAL(17, 4),
                           'benchmark': NVARCHAR(500),
                           'status': NVARCHAR(20),
                           'invest_type': NVARCHAR(255),
                           'type': NVARCHAR(255),
                           'trustee': NVARCHAR(255),
                           'purc_startdate': DATE,
                           'redm_startdate': DATE,
                           'market': NVARCHAR(20)})
    return res


def get_Fund_Basic(db_engine, ts_pro):
    drop_Table(db_engine, prefix)
    get_and_write_data_by_limit(prefix, db_engine, ts_pro, get_data, write_db,
                                rows_limit, times_limit, sleeptimes)


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()

    get_Fund_Basic(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("请复核，场内场外公募基金列表，加载是否正确执行！")
