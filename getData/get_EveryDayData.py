#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 该文件主要用于获取每日数据


import sys
import os

# 获取当前文件的绝对路径（__file__ 表示当前脚本的文件路径）
curPath = os.path.abspath(os.path.dirname(__file__))
# 提取上级目录路径（分割路径得到父级目录）
rootPath = os.path.split(curPath)[0]
# 将项目根目录加入系统路径（解决模块导入问题）
sys.path.append(rootPath)


# 导入其他自定义模块
# 如有新增接口需要在这里导入
from getData.deal_DuplicatetData import deal_duplicate_data
from basis.Init_Env import init_currentDate, init_db, init_ts_pro
from getDataFromTushare.Get_TradeCal_ToDB import get_Trade_Cal

#未验证
from getDataFromTushare.Get_HSGT_North_Top10_ToDB import get_hsgt_north_top10
from getDataFromTushare.Get_Index_Daily_ToDB import get_index_daily
from getDataFromTushare.Get_Stock_Daily_ToDB import get_Stock_Daily
from getDataFromTushare.Get_Index_Basic_ToDB import get_index_basic
from getData.deal_DuplicatetData import deal_duplicate_data
from getData.deal_LostData import deal_lost_data
from getDataFromTushare.Get_Cb_Basics_ToDB import get_Cb_Basic
from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Adj_Factor_ToDB import get_Adj_Factor
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic
from getDataFromTushare.Get_Fund_Basics_ToDB import get_Fund_Basic
from getDataFromTushare.Get_Fund_Daily_ToDB import get_Fund_Daily
from getDataFromTushare.Get_Stock_Basics_ToDB import get_Stock_Basic
from getDataFromTushare.Get_TopInst_ToDB import get_TopInst
from getDataFromTushare.Get_TopList_ToDB import get_TopList
from getDataFromTushare.Get_TradeCal_ToDB import get_Trade_Cal
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Cb_Daily_ToDB import get_Cb_Daily
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code
from getDataFromTushare.Get_Repo_Daily_ToDB import get_Repo_Daily
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Stock_Moneyflow_ToDB import get_Stock_Moneyflow
from getDataFromTushare.Get_Index_Weight_ToDB import get_Index_Weight
from getDataFromTushare.Get_Alternative_CCTV_News_ToDB import get_Alternative_CCTV_News
from getDataFromTushare.Get_HK_Basics_D_ToDB import get_hk_Basic_D
from getDataFromTushare.Get_HK_Basics_ToDB import get_hk_Basic
from getDataFromTushare.Get_HK_CCASS_Hold_Detail_ToDB import get_HK_CCASS_Hold_Detail
from getDataFromTushare.Get_HK_Daily_ToDB import get_HK_Daily
from getDataFromTushare.Get_HK_TradeCal_ToDB import get_HK_Trade_Cal
from getDataFromTushare.Get_Financial_Income_ToDB import get_financial_income
from getDataFromTushare.Get_Cb_Min_ToDB import get_Cb_Min_By_date_and_codelist
from getDataFromTushare.Get_Stock_Min_ToDB import get_stock_Min_By_date_and_codelist
from getDataFromTushare.Get_Stock_STK_Rewards_Fast_ToDB import get_stock_stk_rewards_fast
from getDataFromTushare.Get_stk_factor_pro_Daily_ToDB import Get_stk_factor_pro_Daily_ToDB


def get_data_by_reload_all(db_engine, ts_pro):
    """
    全量更新基础参考数据（清空旧表后重新加载）
    适用场景：初始化数据库、定期维护更新基础信息
    
    Args:
        db_engine: 数据库连接引擎
        ts_pro: 初始化后的Tushare Pro接口对象
    """
    # 交易日历（首次部署时需要，后续可注释）,测试后发现他看起来是按年更新的，直接加载到了20251231
    # get_Trade_Cal(db_engine, ts_pro)  # 加载/更新交易所交易日历

    # --- 股票基础数据 ---
    # get_Stock_Basic(db_engine, ts_pro)  # 上市公司基本信息（代码/名称/上市日期等）
    # get_stock_stk_rewards_fast(db_engine, ts_pro)  # 高管薪酬（需5000积分权限）
    # get_Stock_Daily(db_engine, ts_pro,start_date=str(19901219), end_date=currentDate)  # 日线行情（开盘价/收盘价/成交量等）
    # get_index_daily(db_engine, ts_pro,start_date=str(19901219), end_date=currentDate)  # 指数日线行情
    # get_Stock_Daily_Basic(db_engine, ts_pro,start_date=str(19901219), end_date=currentDate)  # 扩展指标（市盈率/市净率/换手率等）
    Get_stk_factor_pro_Daily_ToDB(db_engine, ts_pro,start_date=str(19901219), end_date=currentDate)  # 股票技术面因子（专业版）
    
    # --- 港股数据（按需启用）---
    # get_HK_Trade_Cal(db_engine, ts_pro)  # 港股交易日历
    # get_hk_Basic(db_engine, ts_pro)     # 港股基本信息
    # get_hk_Basic_D(db_engine, ts_pro)  # 港股每日基本信息
    
    # --- 可转债数据 ---
    # get_Cb_Basic(db_engine, ts_pro)  # 可转债基本信息（转股价/到期日等）
    
    # --- 基金数据 ---
    # get_Fund_Basic(db_engine, ts_pro)  # 公募基金列表（代码/名称/类型）
    # get_Fund_Daily(db_engine, ts_pro,start_date=str(19901219), end_date=currentDate)  # 场内ETF/LOF基金行情
    
    # --- 指数数据 ---
    # get_index_basic(db_engine, ts_pro)  # 指数基本信息（成分股/编制方案等）

def get_data_by_date(db_engine, ts_pro, str_date, end_date):
    """
    按日期范围更新市场交易数据（增量更新模式） 
    适用于每日更新或特定时间段内的数据更新
    Args:
        str_date: 开始日期 (格式：YYYYMMDD)
        end_date: 结束日期 (包含)
        其他参数同 get_data_by_reload_all
    典型场景：每日收盘后执行，更新当日市场数据


    #### 典型调用方式（更新2023-10-01至当日最新数据）
    get_data_by_date(
        db_engine, 
        ts_pro,
        str_date='20231001',
        end_date=datetime.today().strftime('%Y%m%d')
    )

    数据更新频率：
    股票/基金/转债数据 → 每个交易日收盘后更新
    龙虎榜数据 → 晚间21:00左右更新
    新闻数据 → 次日凌晨更新
    """
    # ----------------- 股票市场数据 -----------------
    # get_Stock_Daily(db_engine, ts_pro, str_date, end_date)        # 日线行情（开盘价/收盘价/成交量等）
    # get_Stock_Daily_Basic(db_engine, ts_pro, str_date, end_date)  # 扩展指标（市盈率/市净率/换手率等）
    # get_Adj_Factor(db_engine, ts_pro, str_date, end_date)         # 复权因子（用于计算前复权价格）
    # get_TopInst(db_engine, ts_pro, str_date, end_date)            # 龙虎榜机构买卖明细
    # get_TopList(db_engine, ts_pro, str_date, end_date)            # 龙虎榜汇总数据（每日明细）
    # get_Stock_Moneyflow(db_engine, ts_pro, str_date, end_date)    # 个股资金流向（主力/散户资金）
    # get_cyq_perf(db_engine, ts_pro, str_date, end_date)             # 个股每日筹码及胜率（为测试不可用）

    # ----------------- 港股数据（按需启用）-----------------
    # get_HK_Daily(db_engine, ts_pro, str_date, end_date)         # 港股日线行情（需开通权限）
    # get_HK_CCASS_Hold_Detail(...)                               # 港股中央结算持仓明细（高积分权限）

    # ----------------- 可转债市场 -----------------
    # get_Cb_Daily(db_engine, ts_pro, str_date, end_date)           # 可转债日线行情（转股溢价率等）

    # ----------------- 回购市场 -----------------
    # get_Repo_Daily(db_engine, ts_pro, str_date, end_date)         # 国债逆回购利率数据

    # ----------------- 基金市场 -----------------
    # get_Fund_Daily(db_engine, ts_pro, str_date, end_date)          # 场内ETF/LOF基金行情

    # ----------------- 市场结构数据 -----------------
    # get_Index_Weight(db_engine, ts_pro, str_date, end_date)        # 指数成分股权重（沪深300等）
    # get_hsgt_north_top10(db_engine, ts_pro, str_date, end_date)    # 沪深港通十大成交股

    # ----------------- 新闻舆情数据 -----------------
    # get_Alternative_CCTV_News(db_engine, ts_pro, str_date, end_date)  # 新闻联播文本（事件驱动分析）

    # ----------------- 指数行情 -----------------
    # get_index_daily(db_engine, ts_pro, str_date, end_date)         # 主要指数日线（上证50/沪深300等）

    # ----------------- 高频数据（需特殊权限）-----------------
    # tick 数据，如果需要抓取tick数据解除该处注释即可. 注意：该接口权限需向tushare官方单独购买
    # get_stock_Min_By_date_and_codelist(...)  # 1分钟级行情（需单独付费购买权限）
    # get_Cb_Min_By_date_and_codelist(...)    # 可转债分钟行情


def deal_wrong_date(db_engine, ts_pro, ts, str_date, end_date):
    """
    数据质量维护函数：处理重复数据和缺失数据
    Args:
        ts: 普通版Tushare接口对象（用于高频数据）
        str_date: 开始日期 (格式：YYYYMMDD)
        end_date: 结束日期 (包含)
        其他参数同 get_data_by_reload_all

    处理流程：
    1. 去重阶段 → 根据复合主键删除完全重复的记录
       示例：hq_stock_daily表按 (交易日期, 股票代码) 去重
    2. 补数阶段 → 优先按日期范围补全，失败时按代码逐条补全
       示例：发现2023-10-01缺少贵州茅台数据 → 调用get_Daily_By_Code('600519.SH')

    被注释的表说明：
    港股相关表（hq_hk_daily等）需要额外权限，默认不开启
    高频数据表（hq_cb_min等）需付费权限，需手动开启
    """
    print('===================整理重复数据和该日期段缺失数据======================')
    
    # 定义需要去重的表结构 [表名, 复合主键列]
    tableList_d = [
        ['hq_stock_daily', 'trade_date, ts_code'],           # 股票日线
        ['hq_stock_daily_basic', 'trade_date, ts_code'],      # 股票扩展指标
        ['hq_adj_factor', 'trade_date, ts_code'],             # 复权因子
        ['hq_topinst', 'trade_date, ts_code, exalter, side, net_buy, reason'],  # 机构明细
        ['hq_toplist', 'trade_date, ts_code, reason'],        # 龙虎榜汇总
        ['hq_fund_daily', 'trade_date, ts_code'],             # 基金日线
        ['hq_cb_daily', 'trade_date, ts_code'],               # 可转债日线
        ['hq_repo_daily', 'trade_date, ts_code'],              # 回购利率
        ['hq_index_weight', 'index_code, con_code, trade_date']  # 指数权重
    ]
    
    # 执行重复数据清理（根据复合主键删除重复记录）
    deal_duplicate_data(db_engine, ts_pro, ts, tableList_d, str_date, end_date)
    # 这里，按传入的日期段和接口表列表和对应的函数入口，补全缺失数据,在tableDict_date字典里的接口就是要补全的接口，
    # tableDict_code里面有没有登记没关系，只是个效率优化手段​

    # 数据补全映射表（表名: 数据获取函数）
    tableDict_date = {
        # 'hq_stock_daily': get_Stock_Daily,                    # 按日期补股票日线
        # 'hq_stock_daily_basic': get_Stock_Daily_Basic,        # 补股票指标
        # 'hq_adj_factor': get_Adj_Factor,                     # 补复权因子
        # 'hq_topinst': get_TopInst,                            # 补机构明细
        # 'hq_toplist': get_TopList,                            # 补龙虎榜
        # 'hq_fund_daily': get_Fund_Daily,                      # 补基金数据
        # 'hq_cb_daily': get_Cb_Daily,                         # 补可转债
        # 'hq_repo_daily': get_Repo_Daily,                      # 补回购数据
        # 'hq_index_weight': get_Index_Weight                   # 补指数权重
    }
    
    # 按证券代码补全的映射表（表名: 代码级补数函数）
    tableDict_code = {
        # 'hq_stock_daily_basic': get_Daily_Basic_By_Code,      # 按代码补股票指标
        # 'hq_stock_daily': get_Daily_By_Code,                  # 按代码补日线
        # 'hq_adj_factor': get_Adj_Factor_By_Code,              # 按代码补复权因子
        # 'hq_fund_daily': get_Fund_Daily_By_Code               # 按代码补基金数据
    }
    
    # 执行缺失数据补全（先日期维度后代码维度）
    # deal_lost_data(db_engine, ts_pro, ts, tableDict_date, tableDict_code, str_date, end_date)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()          # 创建数据库连接池
    ts_pro = init_ts_pro()         # 初始化专业版Tushare接口
    #ts = init_ts()                 # 初始化普通版Tushare接口（兼容旧版API） 看了新的文档发现不需要了
    currentDate = init_currentDate()
    # 指定日期是注意日期格式应为：'20210901'
    # str_date = currentDate
    # end_date = currentDate
    str_date = '20250321'
    end_date = '20250321'

    # 加载列表信息，该类接口均为清空后重新加载，其中日期表建议加载一次就可以了，后续客户注释掉
    get_data_by_reload_all(db_engine, ts_pro)  # STEP1: 全量更新基础数据
    # 按日期段加载每日数据
    # get_data_by_date(db_engine, ts_pro, str_date, end_date)  # STEP2: 增量更新当日数据
    # 按日期段进行数据整理
    #deal_wrong_date(db_engine, ts_pro, str_date, end_date) # STEP3: 数据质量校验
    #deal_wrong_date(db_engine, ts_pro, ts, str_date, end_date) # STEP3: 数据质量校验   删除ts参数

    # 执行结果反馈
    print('数据加载完毕，数据日期：', end_date)  # 输出最终日期标记
    end_str = input("今日数据加载完毕，请复核是否正确执行！")  # 阻塞等待用户确认

    # 关闭sqlalchemy连接池
    db_engine.dispose()
    print('数据库连接已关闭！')

    # 每日运行流程：
    # 1. 早上9:00 → 运行全量更新（仅维护时执行）
    # 2. 收盘后18:00 → 增量更新当日数据
    # 3. 23:00 → 执行数据校验
    # 4. 人工复核 → 通过控制台日志检查错误信息

    # 典型耗时（参考配置：4核CPU/16GB内存）：
    # get_data_by_reload_all → 2-5分钟（依赖网络速度）
    # get_data_by_date → 3-10分钟（数据量越大越久）
    # deal_wrong_date → 1-3分钟（数据校验复杂度高）