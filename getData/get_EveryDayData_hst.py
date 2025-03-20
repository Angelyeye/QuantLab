#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#这个文件主要用于获取每日历史数据

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getData.get_EveryDayData import get_data_by_reload_all, get_data_by_date, deal_wrong_date


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()        # SQLAlchemy数据库引擎
    ts_pro = init_ts_pro()       # Tushare Pro接口实例
    ts = init_ts()               # 旧版Tushare接口（兼容保留）
    currentDate = init_currentDate()  # 当前日期对象

    # 日期范围配置（测试用固定日期，正式使用请启用上方currentDate）
    # 指定日期是注意日期格式应为：'20210901'
    # str_date = currentDate
    # end_date = currentDate
    str_date = '20040302'
    end_date = currentDate
    # str_date = '20120101'
    # end_date = '20220121'

    # 加载列表信息，该类接口均为清空后重新加载，其中日期表建议加载一次就可以了
    # get_data_by_reload_all(db_engine, ts_pro)
    # 按日期段加载每日数据
    # get_data_by_date(db_engine, ts_pro, str_date, end_date)
    # 按日期段进行数据整理

    # 核心数据处理流程
    deal_wrong_date(             # 数据清洗修正函数
        db_engine,               # 数据库连接
        ts_pro,                  # Tushare Pro接口
        ts,                      # 旧版接口（参数保留位）
        str_date,                # 处理起始日
        end_date                 # 处理结束日
    )

    # 输出信息
    print('数据加载完毕，数据日期：', end_date)
    end_str = input("今日数据加载完毕，请复核是否正确执行！")
