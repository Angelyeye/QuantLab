#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path


import basis
from basis import constant
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import tushare as ts
from datetime import datetime
import pandas as pd



def init_db():
    """初始化数据库连接引擎"""
    connect_info = constant.get_db_path()  # 从配置获取数据库连接字符串
    engine = create_engine(connect_info)  # 创建SQLAlchemy引擎实例
    init_schema(engine)       # 初始化数据库模式（创建schema）
    init_hq_lost(engine)      # 初始化行情缺失记录表
    print(engine, '数据库链接初始化成功!')  # 输出连接成功信息
    return engine             # 返回数据库引擎对象

def init_ts_pro():
    """创建Tushare Pro接口实例"""
    print('ts_pro:', ts.__version__)  # 打印Tushare版本信息
    token = constant.get_tushare_token()  # 从配置获取专业版token
    ts.set_token(token)       # 设置Tushare全局token
    ts_pro = ts.pro_api()     # 创建Pro接口实例
    print('Tushare Pro接口已经成功初始化', ts_pro)
    return ts_pro             # 返回Pro接口对象

# 看了新的文档发现好像不需要这个了，直接用ts.pro_api()就可以了
def init_ts():
     """初始化基础版Tushare模块"""
     print('ts:', ts.__version__)  # 打印Tushare基础版版本
     token = basis.Constant.get_pro_token()  # 复用Pro版token
     ts.set_token(token)       # 设置全局token（与Pro版共用）
     return ts                 # 返回Tushare模块对象



def init_hq_lost(engine):
    """初始化行情缺失记录表(hq_lost)
    创建用于存储缺失行情数据的表结构，包含以下字段：
    - lost_Type: 缺失类型（如历史数据缺失、实时数据缺失等）
    - trade_Date: 交易日日期
    - ts_Code:   证券代码
    - reason:    缺失原因说明
    """
    sql = 'create table if not exists hq_lost(lost_Type varchar(200) null, ' \
          'trade_Date date null, ts_Code varchar(20) null, reason varchar(200) null)'  # 建表SQL语句
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()  # 提交事务
    print('行情缺失记录表hq_lost已经成功初始化')


def init_schema(engine):
    """初始化数据库模式（schema）
    1. 从连接字符串中提取数据库名称
    2. 创建schema（若不存在）
    3. 处理首次创建数据库时的异常情况
    """
    # 从数据库连接字符串解析数据库名称（格式：dialect://user:pass@host:port/dbname）
    connect_info = constant.get_db_path()
    dbname_start = connect_info.rfind('/')      # 查找最后一个斜杠位置
    dbname = connect_info[dbname_start + 1:]   # 截取数据库名称
    
    # 构建创建schema的SQL语句
    sql = 'create schema if not exists ' + dbname
    
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()  # 提交事务
    except OperationalError:  # 处理数据库不存在的情况，如果第一次使用，找不到QTdatabase会报错，这里捕捉并处理​
        # 当数据库不存在时，调整连接字符串到上级目录
        connect_info = connect_info[0:dbname_start]  # 移除原数据库名
        engine = create_engine(connect_info)         # 创建无数据库名的引擎
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()  # 提交事务
        print('数据库已成功创建')


def init_currentDate():
    """获取系统当前日期并格式化为YYYYMMDD字符串
    返回格式示例：'20230804'"""
    currentDate = datetime.now().strftime('%Y%m%d')  # 生成8位日期字符串
    return currentDate  # 返回标准化日期格式字符串


def init_stock_codeList(engine):
    """获取股票基础信息表中的所有唯一证券代码
    返回格式：包含[ts_code]列的DataFrame对象"""
    sql = """select distinct ts_code from hq_stock_basic order by ts_code """  # 去重并按代码排序的查询语句
    codeList = pd.read_sql_query(sql, engine)  # 执行SQL查询获取代码列表
    return codeList  # 返回股票代码清单DataFrame


def init_cb_codeList(engine):
    """获取可转债基础信息表中的所有唯一证券代码
    返回格式：包含[ts_code]列的DataFrame对象"""
    sql = """select distinct ts_code from hq_cb_basic order by ts_code """  # 可转债代码去重查询语句
    codeList = pd.read_sql_query(sql, engine)  # 执行SQL获取可转债代码
    return codeList  # 返回可转债代码清单

def init_index_codeList(engine):
    """获取指数基础信息表中的所有唯一指数代码
    返回格式：包含[ts_code]列的DataFrame对象
    
    Args:
        engine: 数据库连接引擎对象
        
    Returns:
        pd.DataFrame: 包含排序后唯一指数代码的DataFrame
        示例：
            ts_code
        0   000001.SH
        1   000002.SH
    """
    sql = 'select distinct ts_code from hq_index_basic order by ts_code'  # 指数代码去重查询语句
    codeList = pd.read_sql_query(sql, engine)  # 执行SQL查询获取代码列表
    return codeList  # 返回指数代码清单

def is_working_date(engine, market, idate):
    """判断指定日期是否为交易日
    Args:
        engine:  数据库连接引擎
        market: 市场类型 ['CN'-中国大陆, 'HK'-香港]
        idate:  日期字符串 (格式：YYYYMMDD)
    Returns:
        bool: True-交易日，False-非交易日
    """
    df = pd.DataFrame()
    # 中国大陆市场查询逻辑
    if market == 'CN':
        sql = 'select is_open from trade_cal where exchange = \'SSE\' and cal_date = %s ' % idate  # 查询上交所交易日历
        df = pd.read_sql_query(sql, engine)
    # 香港市场查询逻辑
    elif market == 'HK':
        sql = 'select is_open from hq_hk_trade_cal where cal_date = %s ' % idate  # 查询港交所交易日历
        df = pd.read_sql_query(sql, engine)
    
    # 处理空数据情况（默认返回True保证程序继续执行）
    if df.__sizeof__() == 0:  # 无查询结果时视为交易日
        return True
    else:
        is_open = df.iloc[0][0]  # 获取首个结果的is_open字段值
        if is_open == '1':  # '1'表示交易日
            return True
        else:  # '0'表示非交易日
            return False