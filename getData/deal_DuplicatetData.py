#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#该文件主要用于处理重复数据

import datetime
import time

import pandas as pd

from basis.Init_Env import init_db, init_ts_pro, init_currentDate , init_ts
from get_LostData_By_Code import get_LostData_By_Code
from get_LostData_By_Date import get_LostData_By_Date


def deal_duplicate_in_hq_lost(db_engine, prefix, pkey, str_date, end_date):
    """
    数据库表重复数据清理核心逻辑（不修改现有代码）
    
    Args:
        db_engine: 数据库连接引擎
        prefix: 目标表名（示例：'hq_stock_daily'）
        pkey: 复合主键字段（多个字段用逗号分隔，示例：'trade_date, ts_code'）
        str_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（包含）
    
    Returns:
        pd.DataFrame: 包含重复数据统计结果的DataFrame
        
    Raises:
        SQLAlchemyError: 数据库操作失败时抛出
    """
    # 打印操作日志头
    print('===================================================================')
    print('deal_duplicate_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '开始查找重复数据')
    # 登记可能重复数据，有些情况不属于重复(比如hq_toplist中通日期和代码的数据有不重复的)，为方便通用处理也一并登记
    try:
        sql_str = 'select count(ct) from (select count(*) as ct from %s ' \
                  'where trade_date between \'%s\' and \'%s\' group by  %s ' \
                  'having count(*)>1)rt' % (prefix, str_date, end_date, pkey)
        ilost = pd.read_sql_query(sql_str, db_engine)
    except:  # 如果带日期条件的查询失败，则尝试全表统计
        sql_str = 'select count(*) from %s group by  %s having count(*)>1' % (prefix, pkey)
        ilost = pd.read_sql_query(sql_str, db_engine)
    # 如果没有缺失数据，就不用继续傻做了，节省时间
    if len(ilost.index) == 0 or ilost.iat[0, 0] == 0:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表无重复数据!')
        return ilost
    else:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表有重复数据：', ilost.iat[0, 0], ' 条！')
    # 给原表增加自增主键，便于后续找到重复数据并删除
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '给表增加自增主键，便于后续找到重复数据并删除!')
    sql_str = 'alter table %s add id int auto_increment primary  key ' % prefix
    try:
        with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    except Exception as ex:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '给表增加自增主键出错，由于已经有自增主键，所以无需添加，继续往后处理!')

    # 将重复数据，从原表中移动到临时表tmp_duplicate
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '将数据去重后，登记到临时表tmp_duplicate！')
    sql_str = 'drop table if exists tmp_duplicate'
    with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    try:
        sql_str = 'create table tmp_duplicate select max(id) as id from %s ' \
                  'where trade_date between \'%s\' and \'%s\' ' \
                  'group by  %s ' % (prefix, str_date, end_date, pkey)
        with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    except:
        sql_str = 'create table tmp_duplicate select max(id) as id from %s tb ' \
                  'group by  %s ' % (prefix, pkey)
        with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()

    # 将数据从原表中删除
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '将重复数据从原表中删除！')
    try:
        sql_del = 'delete from %s a where a.trade_date between \'%s\' and \'%s\' ' \
                  'and a.id not in (select b.id from tmp_duplicate b)' % (prefix, str_date, end_date)
        res = db_engine.execute(sql_del)
    except:
        sql_del = 'delete from %s a where a.id not in (select b.id from tmp_duplicate b)' % prefix
        res = db_engine.execute(sql_del)

    # drop 临时表 tmp_duplicate
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '删除临时表 tmp_duplicate！')
    sql_str = 'drop table if exists tmp_duplicate'
    with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    # 去掉原表中的自增主键id
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '恢复原表表结构！')
    sql_str = 'alter table %s DROP COLUMN id' % prefix
    with db_engine.connect() as conn:
            conn.execute(text(sql_str))
            conn.commit()
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '已完成对该表指定日期段，重复数据的去重处理！')

def deal_duplicate_data(db_engine, ts_pro, ts, tableList, str_date, end_date):
    """
    重复数据处理入口函数  （登记重复数据）
    
    Args:
        db_engine: 数据库连接引擎
        ts_pro: Tushare Pro接口对象（保留参数）
        ts: Tushare旧版接口对象（保留参数）
        tableList: 待处理表配置列表，格式为[[表名, 主键字段], ...]
        str_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（包含）
    """
    # 遍历配置列表中的表处理规则
    for tb_pkey in tableList:
        # 解构表配置：表名和主键字段
        tb = tb_pkey[0]    # 目标表名称
        pkey = tb_pkey[1]  # 该表的复合主键字段
        # 调用核心处理函数进行去重操作
        deal_duplicate_in_hq_lost(db_engine, tb, pkey, str_date, end_date)


if __name__ == '__main__':
    """
    主程序入口 - 重复数据清理执行模块
    
    执行流程：
    1. 初始化数据库连接和API接口
    2. 配置需要处理的表及其主键规则
    3. 调用核心处理函数执行去重操作
    """
    # 初始化基础环境
    db_engine = init_db()            # 获取数据库连接引擎
    ts_pro = init_ts_pro()           # 初始化Tushare Pro接口
    currentDate = init_currentDate() # 获取当前日期(YYYYMMDD格式)
    str_date = '20000101'            # 设置起始日期为2000年1月1日
    end_date = currentDate           # 结束日期设置为当前日期
    ts = init_ts()                   # 初始化旧版Tushare接口(保留备用)

    # 用一个二维数组装[接口名，主键值]的列表
    # 配置需要处理的表及其主键规则（格式：[表名, 复合主键]）
    # 当前仅启用'hq_alternative_anns'表作为示例，其他表配置已被注释
    ableList_d = [
        # 公告替代数据表 | 主键：股票代码+公告日期+标题
        ['hq_alternative_anns', 'ts_code, ann_date, title'],
        # 以下为其他表配置示例（需要时取消注释即可启用）：
        # ['hq_stock_daily', 'trade_date, ts_code'],                                           # 股票日线表
        # ['hq_stock_daily_basic', 'trade_date, ts_code'],                                     # 股票基本指标表
        # ['hq_adj_factor', 'trade_date, ts_code'],                                            # 复权因子表
        # ['hq_topinst', 'trade_date, ts_code, exalter, side, net_buy, reason'],               # 机构明细表
        # ['hq_topinst', 'trade_date, ts_code, exalter, side, net_buy, reason'],
        # ['hq_toplist', 'trade_date, ts_code, reason'],
        # ['hq_fund_daily', 'trade_date, ts_code'],
        # ['hq_hsgt_north_top10', 'trade_date,ts_code,market_type'],
        # ['hq_cb_daily', 'trade_date, ts_code'],
        # ['hq_repo_daily', 'trade_date, ts_code'],
        # ['hq_financial_income', 'ts_code, ann_date, f_ann_date, end_date, update_flag'],
        # ['hq_alternative_cctv_news', 'date, title'],
        # ['hq_index_weight', 'index_code, con_code, trade_date'],
        # ['hq_stock_moneyflow', 'trade_date, ts_code']
    ]

    # 执行核心去重流程
    deal_duplicate_data(
        db_engine, 
        ts_pro, 
        ts, 
        ableList_d,   # 传入表配置列表
        str_date,     # 起始日期参数
        end_date      # 结束日期参数
    )