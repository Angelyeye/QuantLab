'''
Created on 2021年10月18日



可转债基本信息
接口：cb_basic
描述：获取可转债基本信息
限量：单次最大2000，总量不限制
权限：用户需要至少2000积分才可以调取，但有流量控制，5000积分以上频次相对较高，积分越多权限越大，具体请参阅积分获取办法


输入参数

名称	类型	必选	描述
ts_code	str	N	转债代码
list_date	str	N	上市日期
exchange	str	N	上市地点


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	转债代码
bond_full_name	str	Y	转债名称
bond_short_name	str	Y	转债简称
cb_code	str	Y	转股申报代码
stk_code	str	Y	正股代码
stk_short_name	str	Y	正股简称
maturity	float	Y	发行期限（年）
par	float	Y	面值
issue_price	float	Y	发行价格
issue_size	float	Y	发行总额（元）
remain_size	float	Y	债券余额（元）
value_date	str	Y	起息日期
maturity_date	str	Y	到期日期
rate_type	str	Y	利率类型
coupon_rate	float	Y	票面利率（%）
add_rate	float	Y	补偿利率（%）
pay_per_year	int	Y	年付息次数
list_date	str	Y	上市日期
delist_date	str	Y	摘牌日
exchange	str	Y	上市地点
conv_start_date	str	Y	转股起始日
conv_end_date	str	Y	转股截止日
first_conv_price	float	Y	初始转股价
conv_price	float	Y	最新转股价
rate_clause	str	Y	利率说明
put_clause	str	N	赎回条款
maturity_put_price	str	N	到期赎回价格(含税)
call_clause	str	N	回售条款
reset_clause	str	N	特别向下修正条款
conv_clause	str	N	转股条款
guarantor	str	N	担保人
guarantee_type	str	N	担保方式
issue_rating	str	N	发行信用等级
newest_rating	str	N	最新信用等级
rating_comp	str	N	最新评级机构
'''
import time

from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 15000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'hq_cb_basic'


@retry(tries=2, delay=61)
def get_data(ts_pro, rows_limit, offset):
    df = ts_pro.cb_basic(limit=rows_limit, offset=offset)
    return df


def write_db(df, db_engine):
    res = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=100000,
                    dtype={'ts_code': NVARCHAR(20),
                           'bond_full_name': NVARCHAR(500),
                           'bond_short_name': NVARCHAR(250),
                           'cb_code': NVARCHAR(20),
                           'stk_code': NVARCHAR(20),
                           'stk_short_name': NVARCHAR(250),
                           'maturity': DECIMAL(17, 4),
                           'par': DECIMAL(17, 4),
                           'issue_price': DECIMAL(17, 4),
                           'issue_size': DECIMAL(17, 4),
                           'remain_size': DECIMAL(17, 4),
                           'value_date': DATE,
                           'maturity_date': DATE,
                           'rate_type': NVARCHAR(250),
                           'coupon_rate': DECIMAL(17, 4),
                           'add_rate': DECIMAL(17, 4),
                           'pay_per_year': DECIMAL(17, 4),
                           'list_date': DATE,
                           'delist_date': DATE,
                           'exchange': NVARCHAR(20),
                           'conv_start_date': DATE,
                           'conv_end_date': DATE,
                           'first_conv_price': DECIMAL(17, 4),
                           'conv_price': DECIMAL(17, 4),
                           'rate_clause': NVARCHAR(2000)})
    return res


def get_Cb_Basic(db_engine, ts_pro):
    drop_Table(db_engine, prefix)
    get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()

    get_Cb_Basic(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("数据加载完毕，加载是否正确执行！")
