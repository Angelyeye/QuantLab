#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 该文件主要用于按日期补全缺失数据

import pandas as pd
# 在文件开头添加以下代码
import sys
from pathlib import Path
from sqlalchemy import text

# 将项目根目录添加到Python路径
sys.path.append(str(Path(__file__).parent.parent))

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic


def init_LostList_by_date(db_engine):
    """[核心方法] 获取待补全的日期缺失数据列表
    
    参数：
        db_engine -- 数据库连接引擎（SQLAlchemy引擎实例）
        
    返回值：
        DataFrame 包含两列：
            lost_Type - 缺失数据类型（对应数据库表名）
            trade_Date - 需要补全的日期（datetime类型）
            
    处理逻辑：
        1. 从hq_lost表中获取唯一的缺失记录类型和对应日期
        2. 按缺失类型降序、日期升序排序（便于优先处理重要数据类型）
        
    示例：
        lost_dates = init_LostList_by_date(db_engine)
    """
    # 构造SQL查询（获取唯一缺失记录类型及日期，按类型倒序/日期正序排列）
    sql = """select distinct lost_Type,trade_Date from hq_lost order by lost_Type desc,trade_Date """
    # 执行查询并返回结果集（转换为DataFrame）
    lostList = pd.read_sql_query(sql, db_engine)
    return lostList


def get_LostData_by_date_and_lost_Type(db_engine, ts_pro, get_data, trade_Date, lost_Type):
    """[核心方法] 按日期补全指定类型的缺失数据
    
    参数：
        db_engine   -- 数据库连接引擎 (SQLAlchemy)
        ts_pro      -- Tushare Pro接口对象
        get_data    -- 数据获取函数（包含入库逻辑）
        trade_Date  -- 需要补全的日期 (datetime对象)
        lost_Type   -- 缺失数据类型/目标表名
    
    处理流程：
        1. 清理目标表旧数据 -> 2. 加载新数据 -> 3. 更新缺失记录状态
    """
    # 步骤1：日期格式转换（datetime转Tushare格式）
    idate = str(trade_Date.strftime('%Y%m%d'))  # 输出示例：20211105
    
    # 步骤2：清理目标表旧数据（防止重复数据）
    # 构建删除SQL：DELETE FROM [表名] WHERE trade_date = [日期]
    sql = "delete from %s where trade_date = \'%s\' " % (lost_Type, idate)
    with db_engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print('get_LostData_By_Date.', lost_Type, ':已删除表中trade_date为：', idate, '的数据')
    
    # 步骤3：执行数据获取（核心数据加载入库）
    # 调用对应表的数据获取函数，参数：(数据库连接, Tushare接口, 开始日期, 结束日期)
    get_data(db_engine, ts_pro, idate, idate)
    
    # 步骤4：更新缺失记录状态（从hq_lost表移除已处理记录）
    # 构建删除SQL：DELETE FROM hq_lost WHERE lost_Type=类型 AND trade_Date=日期
    sql = "delete from hq_lost where lost_Type = \'%s\' and trade_Date = \'%s\'  " % (lost_Type, idate)
    with db_engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print('get_LostData_By_Date.', lost_Type, ':补全数据后，已删除hq_lost中lost_Type为', lost_Type, '，trade_date日期为', idate,
          ' 的数据')


def get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date):
    """[主函数] 按日期补全所有类型缺失数据
    
    参数：
        db_engine     -- 数据库连接引擎
        ts_pro        -- Tushare Pro接口对象
        ts            -- Tushare旧版接口（当前版本未使用，保留参数位）
        tableDict_date -- 数据表与处理函数的映射字典
                         key: 表名，value: 数据处理函数
    
    处理流程：
        1. 初始化缺失数据列表 -> 2. 遍历处理每个缺失项 -> 3. 匹配处理模块
        4. 找不到模块时输出告警
    
    注意事项：
        需要确保tableDict_date已配置所有可能的数据类型处理函数
    """
    # 从数据库读取待补全的日期列表（格式：DataFrame转为numpy数组）  读取缺失数据列表
    lostListArray = init_LostList_by_date(db_engine).__array__()

    # 遍历每个缺失记录（lost_Type: 表名, trade_Date: 日期）  补充缺失数据
    for lost_Type, trade_Date in lostListArray:
        # 检查是否配置了该类型的数据处理模块
        if lost_Type in tableDict_date.keys():
            # 执行具体类型的数据补全（传递对应的数据处理函数）
            get_LostData_by_date_and_lost_Type(
                db_engine, 
                ts_pro,
                tableDict_date[lost_Type],  # 从映射字典获取处理函数
                trade_Date,
                lost_Type
            )
        else:
            # 输出未找到处理模块的警告（需要人工介入配置）
            print('get_LostData_By_Date：找不到：', lost_Type,
                  '接口的处理模块！请到get_EveryDayData.py的tableDict_date数组中添加处理模块！')


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()        # SQLAlchemy数据库引擎对象
    ts_pro = init_ts_pro()       # Tushare Pro接口实例
    ts = init_ts()               # 旧版Tushare接口（兼容保留）
    currentDate = init_currentDate()  # 当前日期对象
    
    # 测试用日期范围配置（正式使用时注释掉，启用上方currentDate）
    # str_date = currentDate    # <- 生产环境日期配置
    # end_date = currentDate    # <- 单日模式配置
    str_date = '20200101'       # 测试起始日(格式：YYYYMMDD)
    # end_date = '20211110' 
    end_date = currentDate      # 测试结束日
    
    # 数据处理器映射表（key: 表名，value: 数据获取函数）
    tableDict_date = {'hq_stock_daily': get_Stock_Daily_Basic}  # 股票日线数据处理器
    
    # 执行主补全流程（参数说明：数据库连接，Pro接口，旧版接口，处理器映射）
    get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)

    # 终端输出提示
    print('数据加载完毕，数据日期：', end_date)  # 输出最终处理日期
    # end_str = input("今日数据加载完毕，请复核是否正确执行！")  # 原复核确认提示