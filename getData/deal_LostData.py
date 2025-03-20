#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 该文件用于处理缺失数据

import datetime
import time

import pandas as pd
from sqlalchemy import text

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getData.get_LostData_By_Code import get_LostData_By_Code
from getData.get_LostData_By_Date import get_LostData_By_Date
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Adj_Factor_ToDB import get_Adj_Factor
from getDataFromTushare.Get_Cb_Daily_ToDB import get_Cb_Daily
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code
from getDataFromTushare.Get_Fund_Daily_ToDB import get_Fund_Daily
from getDataFromTushare.Get_HSGT_North_Top10_ToDB import get_hsgt_north_top10
from getDataFromTushare.Get_Index_Weight_ToDB import get_Index_Weight
from getDataFromTushare.Get_Repo_Daily_ToDB import get_Repo_Daily
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Stock_Daily_ToDB import get_Stock_Daily
from getDataFromTushare.Get_Stock_Moneyflow_ToDB import get_Stock_Moneyflow
from getDataFromTushare.Get_TopInst_ToDB import get_TopInst
from getDataFromTushare.Get_TopList_ToDB import get_TopList


def insert_lost_into_hq_lost_by_date(db_engine, prefix, str_date, end_date):
    """
    检测并登记指定时间段内的缺失数据到hq_lost表
    
    参数：
        db_engine (sqlalchemy.engine.Engine): 数据库连接引擎
        prefix (str): 目标表前缀（如'hq_stock_daily'），需满足以下条件：
            - 表需在trade_cal中存在对应的交易日记录
            - 表应至少包含trade_date字段（部分表需包含ts_code字段）
        str_date (str): 开始日期（格式：'YYYYMMDD'）
        end_date (str): 结束日期（格式：'YYYYMMDD'）

    返回：
        pandas.DataFrame: 包含统计结果的DataFrame，结构：
            - count: 缺失数据数量（可通过.iat[0,0]获取数值）

    注意：
        1. 自动适配两种表结构：
           - 包含ts_code字段的表（如股票日线数据）
           - 仅含trade_date字段的表（如指数数据）
        2. 日志输出包含完整处理过程记录
        3. 实际写入数据库时会自动跳过已存在记录
        4. 日期范围建议不超过1个季度，避免SQL性能问题
    """
    # 打印处理日志头
    print('===================================================================')
    print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '缺失数据登记')
    try:
        # 尝试第一种SQL插入方式（适用于包含ts_code字段的表）
        sql_str = 'insert into hq_lost select \'%s\' as lost_Type, jg.cal_date, jg.ts_code, \'lost\' as reason from ' \
                  '(select * from (select cal_date from trade_cal where is_open = \'1\' and exchange = \'SSE\' ' \
                  'and cal_date between \'%s\' and \'%s\') cal left join %s tb on cal.cal_date = tb.trade_date ) jg ' \
                  'where jg.trade_date is null' % (prefix, str_date, end_date, prefix)
        with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    except:
        try:
            # 第二种SQL插入方式（适用于不含ts_code字段的表）
            sql_str = 'insert into hq_lost select \'%s\' as lost_Type, ' \
                      'jg.cal_date as trade_Date, \' \' as ts_Code, \'lost\' as reason from ' \
                      '(select * from (select cal_date from trade_cal where is_open = \'1\' and exchange = \'SSE\' ' \
                      'and cal_date between \'%s\' and \'%s\') cal left join %s tb on cal.cal_date = tb.trade_date ) jg ' \
                      'where jg.trade_date is null' % (prefix, str_date, end_date, prefix)
            with db_engine.connect() as conn:
                conn.execute(text(sql_str))
                conn.commit()
        except:
            # 异常处理：打印无法识别表结构的警告
            print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '无法识别该表在该段日期中是否有缺失数据，可能是该表缺少trade_date日期字段!')
    
    # 查询并统计缺失数据量
    sql = sql = 'select count(*) from hq_lost where reason = \'lost\' and lost_Type = \'%s\'' % prefix
    ilost = pd.read_sql_query(sql, db_engine)
    
    # 根据查询结果输出不同日志
    if ilost.iat[0, 0] == 0:
        print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表在日期段：', str_date, '至', end_date, '期间无缺失数据!')
        return ilost
    else:
        print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表', '在日期段：', str_date, '至', end_date, '期间，有缺失数据：', ilost.iat[0, 0], ' 条！',
              '缺失数据所对应的日期，已登记到丢失数据表hq_lost中，后续数据补全程序会补全数据！')


def deal_lost_data(db_engine, ts_pro, ts, tableDict_date, tableDict_code, str_date, end_date):
    """
    缺失数据处理总控函数（数据登记+补全）
    
    参数：
        db_engine: 数据库连接引擎
        ts_pro: Tushare Pro API对象
        ts: Tushare API对象（兼容老接口）
        tableDict_date: 按日期补全的表字典 {表名: 数据获取函数}
        tableDict_code: 按代码补全的表字典 {表名: 数据获取函数}
        str_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（格式：YYYYMMDD）
    
    逻辑说明：
        1. 两阶段处理：先登记所有缺失数据 -> 再执行数据补全
        2. 补全顺序策略：根据缺失类型数量动态决定处理顺序
           - 当代码维度缺失 <= 日期维度缺失时：先补代码维度 -> 后补日期维度
           - 否则：先补日期维度 -> 后补代码维度
    """
    # 第一阶段：遍历所有需要按日期处理的表，登记缺失数据
    for tb in tableDict_date:
        insert_lost_into_hq_lost_by_date(db_engine, tb, str_date, end_date)
    
    # 第二阶段：数据补全（带策略的顺序执行）
    print('===================================================================')
    print('deal_lost_data', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '开始补全')
    
    # 获取两种缺失类型的数量
    icode = how_many_by_code(db_engine).iat[0, 0]  # 代码维度缺失量
    idate = how_many_by_date(db_engine).iat[0, 0]  # 日期维度缺失量
    
    # 动态决定补全顺序（优先处理数量较少的缺失类型）
    if (icode <= idate):
        # 先补代码维度缺失（股票/基金等个体数据）
        get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)
        # 后补日期维度缺失（市场整体数据）
        get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)
    else:
        # 先补日期维度缺失（指数/宏观数据）
        get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)
        # 后补代码维度缺失（个股财务数据等）
        get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)


def how_many_by_code(db_engine):
    """
    统计代码维度缺失数据量
    
    参数：
        db_engine: 数据库连接引擎
        
    返回：
        pandas.DataFrame: 包含统计结果的DataFrame，结构：
            - count: 唯一（lost_Type + ts_Code）组合的缺失数据数量
            
    说明：
        用于获取需要按股票代码/基金代码等个体维度补全的数据量
    """
    # 执行复合维度统计（按缺失类型+证券代码去重统计）
    sql = """select count(distinct lost_Type,ts_Code) from hq_lost where reason = \'lost\' """
    ibycode = pd.read_sql_query(sql, db_engine)  # 结果存储在DataFrame中
    return ibycode


def how_many_by_date(db_engine):
    """
    统计日期维度缺失数据量
    
    参数：
        db_engine: 数据库连接引擎
        
    返回：
        pandas.DataFrame: 包含统计结果的DataFrame，结构：
            - count: 唯一（lost_Type + trade_Date）组合的缺失数据数量
            
    说明：
        用于获取需要按交易日维度补全的数据量，适用于市场整体数据缺失统计
    """
    # 执行复合维度统计（按缺失类型+交易日期去重统计）
    sql = """select count(distinct lost_Type,trade_Date) from hq_lost where reason = \'lost\'  """
    ibydate = pd.read_sql_query(sql, db_engine)  # 结果存储在DataFrame中
    return ibydate


if __name__ == '__main__':
    """
    主程序入口（直接运行时执行）
    功能：执行全量历史数据缺失检测与补全
    """
    # 初始化核心组件
    db_engine = init_db()          # 创建数据库连接引擎
    ts_pro = init_ts_pro()         # 初始化Tushare Pro API (v1.0+)
    ts = init_ts()                 # 初始化旧版Tushare API (v0.x兼容)
    currentDate = init_currentDate()  # 获取当前交易日（格式：YYYYMMDD）
    
    # 配置参数
    str_date = '20120101'         # 起始日期（默认从2012年开始）
    end_date = currentDate        # 结束日期（当前最新日期）
    
    # 定义数据表处理映射（注释掉的表为示例保留项）
    tableDict_date = {
        'hq_stock_daily': get_Stock_Daily,          # 股票日线数据
        'hq_stock_daily_basic': get_Stock_Daily_Basic,  # 股票每日指标
        'hq_adj_factor': get_Adj_Factor,            # 复权因子
        # 'hq_hsgt_north_top10': get_hsgt_north_top10,  # 沪深港通十大成交股（示例）
        'hq_cb_daily': get_Cb_Daily,                # 可转债日线
        'hq_repo_daily': get_Repo_Daily,
        'hq_index_weight': get_Index_Weight          # 指数成分股权重
        # 'hq_stock_moneyflow': get_Stock_Moneyflow,
        # 'hq_alternative_cctv_news': get_Alternative_CCTV_News
        # 'hq_cb_min': get_Cb_Min_By_date_and_codelist,
        # 'hq_stock_min': get_stock_Min_By_date_and_codelist
    }
    
    tableDict_code = {
        'hq_stock_daily_basic': get_Daily_Basic_By_Code,  # 按代码获取每日指标
        'hq_stock_daily': get_Daily_By_Code,             # 按代码获取股票日线数据
        'hq_adj_factor': get_Adj_Factor_By_Code,     # 按代码获取复权因子
        'hq_fund_daily': get_Fund_Daily_By_Code       # 按代码获取基金数据
        # 'hq_stock_moneyflow': get_financial_income
    }
    
    # 执行核心处理流程
    deal_lost_data(
        db_engine, 
        ts_pro, 
        ts,
        tableDict_date,   # 按日期维度处理的表
        tableDict_code,   # 按代码维度处理的表
        str_date,         # 起始日期参数
        end_date          # 结束日期参数
    )