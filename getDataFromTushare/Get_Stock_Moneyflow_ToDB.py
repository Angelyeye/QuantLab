'''
Created on 2021年7月18日



个股资金流向
接口：moneyflow，可以通过数据工具调试和查看数据。
描述：获取沪深A股票资金流向数据，分析大单小单成交情况，用于判别资金动向
限量：单次最大提取4500行记录，总量不限制
积分：用户需要至少2000积分才可以调取，基础积分有流量控制，积分越多权限越大，请自行提高积分，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码 （股票和时间参数至少输入一个）
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
trade_date	str	Y	交易日期
buy_sm_vol	int	Y	小单买入量（手）
buy_sm_amount	float	Y	小单买入金额（万元）
sell_sm_vol	int	Y	小单卖出量（手）
sell_sm_amount	float	Y	小单卖出金额（万元）
buy_md_vol	int	Y	中单买入量（手）
buy_md_amount	float	Y	中单买入金额（万元）
sell_md_vol	int	Y	中单卖出量（手）
sell_md_amount	float	Y	中单卖出金额（万元）
buy_lg_vol	int	Y	大单买入量（手）
buy_lg_amount	float	Y	大单买入金额（万元）
sell_lg_vol	int	Y	大单卖出量（手）
sell_lg_amount	float	Y	大单卖出金额（万元）
buy_elg_vol	int	Y	特大单买入量（手）
buy_elg_amount	float	Y	特大单买入金额（万元）
sell_elg_vol	int	Y	特大单卖出量（手）
sell_elg_amount	float	Y	特大单卖出金额（万元）
net_mf_vol	int	Y	净流入量（手）
net_mf_amount	float	Y	净流入额（万元）

各类别统计规则如下：
小单：5万以下 中单：5万～20万 大单：20万～100万 特大单：成交额>=100万



接口示例


pro = ts.pro_api('your token')

#获取单日全部股票数据
df = pro.moneyflow(trade_date='20190315')

#获取单个股票数据
df = pro.moneyflow(ts_code='002149.SZ', start_date='20190115', end_date='20190315')



数据示例

        ts_code trade_date  buy_sm_vol  buy_sm_amount  sell_sm_vol  \
0     000779.SZ   20190315       11377        1150.17        11100
1     000933.SZ   20190315       94220        4803.22       105924
2     002270.SZ   20190315       43979        2330.96        45893
3     002319.SZ   20190315       21502        2952.88        17155
4     002604.SZ   20190315       31944         607.35        58667
5     300065.SZ   20190315       16048        2294.71        16425
6     600062.SH   20190315       55439        7432.13        65765
7     002735.SZ   20190315        3220         797.10         4598
8     300196.SZ   20190315       12534        1286.02         8340
9     300350.SZ   20190315       15346        1120.12        18853
10    600193.SH   20190315       12183         503.73        19576
11    002866.SZ   20190315       16932        2213.68        16037
12    300481.SZ   20190315       21386        4275.33        21863
13    600527.SH   20190315      115462        2975.44        79272
14    603980.SH   20190315       13957        1924.69        11718
15    600658.SH   20190315       71767        4826.73        69535
16    600812.SH   20190315       26140        1247.47        34923
17    002013.SZ   20190315      170234       12286.02       148509
18    600789.SH   20190315      211012       21644.56       150598
19    601636.SH   20190315       70737        3117.43        68073
20    000807.SZ   20190315      129668        6361.06       122077

...

     sell_sm_amount  buy_md_vol  buy_md_amount  sell_md_vol  sell_md_amount  \
0            1122.97       13012        1316.72        14812         1498.90
1            5411.72      135976        6935.40       154023         7863.00
2            2435.98       57679        3059.15        47279         2507.55
3            2358.68       27245        3742.52        26708         3670.05
4            1114.40       69897        1327.41        41108          781.19
5            2353.34       31232        4472.05        26771         3834.95
6            8817.75       86617       11615.40        79551        10676.99
7            1140.61        4602        1141.61         2730          676.72
8             855.45        9401         963.72        10478         1074.32
9            1380.31       24224        1770.90        21588         1577.92
10            812.58       28696        1185.17        31087         1286.11
11           2100.70       19197        2511.62        20269         2650.56
12           4379.14       31692        6345.72        32873         6578.36
13           2046.54      107103        2763.00        84883         2191.24
14           1619.33       14621        2019.41        14528         2005.69
15           4691.29       92788        6232.80        93273         6280.13
16           1669.97       38812        1855.78        39211         1874.05
17          10726.22      154979       11190.69       164090        11855.76
18          15479.08      269470       27660.18       236958        24338.36
19           3000.73       90416        3984.68       115162         5075.50
20           5999.66      175692        8627.77       178044         8751.08

'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_stock_moneyflow'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'buy_sm_vol': DECIMAL(17, 2),
                                'buy_sm_amount': DECIMAL(17, 2),
                                'sell_sm_vol': DECIMAL(17, 2),
                                'sell_sm_amount': DECIMAL(17, 2),
                                'buy_md_vol': DECIMAL(17, 2),
                                'buy_md_amount': DECIMAL(17, 2),
                                'sell_md_vol': DECIMAL(17, 2),
                                'sell_md_amount': DECIMAL(17, 2),
                                'buy_lg_vol': DECIMAL(17, 2),
                                'buy_lg_amount': DECIMAL(17, 2),
                                'sell_lg_vol': DECIMAL(17, 2),
                                'sell_lg_amount': DECIMAL(17, 2),
                                'buy_elg_vol': DECIMAL(17, 2),
                                'buy_elg_amount': DECIMAL(17, 2),
                                'sell_elg_vol': DECIMAL(17, 2),
                                'sell_elg_amount': DECIMAL(17, 2),
                                'net_mf_vol': DECIMAL(17, 2),
                                'net_mf_amount': DECIMAL(17, 2)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.moneyflow(start_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Stock_Moneyflow(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '20211201'
    end_date = currentDate

    get_Stock_Moneyflow(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日数据加载完毕，请复核是否正确执行！")
