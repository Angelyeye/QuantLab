#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 该文件主要用于按代码补全缺失数据

import os
import sys

import pandas as pd
from sqlalchemy import text

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code

# 获取当前文件所在目录的绝对路径
curPath = os.path.abspath(os.path.dirname(__file__))
# 提取上级目录路径（项目根目录）
rootPath = os.path.split(curPath)[0]
# 将项目根目录添加到系统路径，确保模块正确导入
sys.path.append(rootPath)


def init_LostList_by_code(db_engine):
    """初始化缺失数据列表（按证券代码分类）
    
    参数：
        db_engine -- 数据库连接引擎
        
    返回值：
        DataFrame 包含两列：
            lost_Type -- 缺失数据类型（表名）
            ts_Code   -- 证券代码
            
    示例：
        lost_df = init_LostList_by_code(db_engine)
    """
    # 从hq_lost表获取唯一缺失记录类型和证券代码
    sql = """select distinct lost_Type,ts_Code from hq_lost 
             where ts_Code is not null 
             order by lost_Type ,ts_Code """
    lostList = pd.read_sql_query(sql, db_engine)
    return lostList


def get_LostData_by_code_and_lost_Type(db_engine, ts_pro, get_data, ts_code, lost_Type):
    """[核心方法] 按证券代码和缺失类型补全数据
    
    参数：
        db_engine -- 数据库连接对象
        ts_pro    -- Tushare Pro接口对象
        get_data  -- 数据获取函数（来自tableDict_code映射）
        ts_code   -- 证券代码 (格式：000001.SZ)
        lost_Type -- 缺失类型/表名 (如hq_stock_daily)
        
    处理流程：
        1. 清理目标表中旧数据
        2. 调用对应接口获取最新数据
        3. 更新缺失记录状态
    """
    # 步骤1：清理目标表旧数据（防止重复）
    sql = "delete from %s where ts_code = \'%s\' " % (lost_Type, ts_code)
    with db_engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print('get_LostData_By_Code.', lost_Type, ':已删除表中ts_code为：', ts_code, '的数据')
    
    # 步骤2：执行数据获取函数（核心数据加载）
    get_data(db_engine, ts_pro, ts_code)
    
    # 步骤3：更新缺失记录状态（标记已处理）  删除lost数据库数据
    sql = "delete from hq_lost where lost_Type = \'%s\' and ts_code = \'%s\'  " % (lost_Type, ts_code)
    with db_engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print('get_LostData_By_Code.', lost_Type, ':补全数据后，已删除hq_lost中lost_Type为',
          lost_Type, '，ts_code为', ts_code, '的数据')


def get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code):
    """主控流程：按证券代码补全所有类型缺失数据
    
    参数：
        db_engine     -- 数据库连接对象
        ts_pro        -- Tushare Pro接口
        ts            -- Tushare旧版接口（当前未使用，保留参数位）
        tableDict_code -- 数据表与处理函数的映射字典
                         key: 表名，value: 数据处理函数
    
    处理流程：
        1. 从数据库初始化缺失列表
        2. 遍历所有缺失记录
        3. 根据缺失类型匹配处理函数
        4. 找不到处理函数时输出告警
    
    备注：
        该函数是脚本的主入口，由最后的__main__代码块调用
    """
    # 从数据库读取待补全的证券代码列表
    lostListArray = init_LostList_by_code(db_engine).__array__()
    
    # 遍历每个缺失记录进行补全（lost_Type: 表名, trade_Date: 证券代码）
    for lost_Type, trade_Date in lostListArray:
        # 检查是否存在对应的数据处理模块
        if lost_Type in tableDict_code.keys():
            # 执行具体类型的数据补全操作
            get_LostData_by_code_and_lost_Type(db_engine, ts_pro, 
                                             tableDict_code[lost_Type], 
                                             trade_Date, lost_Type)
        else:
            # 输出未找到处理模块的警告（需要人工处理）
            print('get_LostData_By_Code：找不到：', lost_Type,
                  '接口的处理模块！请到get_EveryDayData.py的函数中添加处理模块！（如添加了table_Dict_date,该告警可忽略）')


if __name__ == '__main__':
    # ------------------------- 初始化连接对象 -------------------------
    # 创建数据库连接引擎（通过Init_Env模块的初始化方法）
    db_engine = init_db()        # 返回SQLAlchemy引擎实例
    ts_pro = init_ts_pro()       # 初始化Tushare Pro接口对象
    ts = init_ts()               # 初始化旧版Tushare接口（当前未实际使用）
    currentDate = init_currentDate()  # 获取当前交易日日期对象

    # ----------------------- 配置数据获取映射关系 -----------------------
    # 定义缺失数据类型与处理函数的映射字典
    # 键: 数据库表名，值: 对应的数据获取函数
    tableDict_code = {
        'hq_stock_daily_basic': get_Daily_Basic_By_Code,  # 股票日基本面数据
        'hq_stock_daily': get_Daily_By_Code,              # 股票日线数据
        'hq_adj_factor': get_Adj_Factor_By_Code,          # 复权因子数据
        'hq_fund_daily': get_Fund_Daily_By_Code           # 基金日线数据
    }

    # ------------------------- 执行主处理流程 --------------------------
    # 调用核心补全函数，遍历处理所有缺失的证券代码数据
    get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)

    # ------------------------- 完成提示信息 --------------------------
    # 输出完成日志（包含当前处理日期）
    print('数据加载完毕，数据日期：', currentDate)
    # 原复核确认提示（当前已被注释）
    # end_str = input("今日数据加载完毕，请复核是否正确执行！")
