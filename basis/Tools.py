#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
# import math
import time

import pandas as pd
# from retry import retry
from sqlalchemy import text, inspect

from basis.Init_Env import init_currentDate, is_working_date

currentDate = init_currentDate()

def drop_Table(db_engine, prefix):
    """删除指定数据表（若存在）"""
    sql_str = 'drop table if exists ' + prefix  # 生成删除表SQL语句
    with db_engine.connect() as conn:          # 使用连接上下文管理器
        conn.execute(text(sql_str))            # 执行SQL命令（使用text函数包装SQL）
        conn.commit()                          # 提交事务
    print(prefix, '数据表，已删除:', sql_str)     # 输出操作日志

def truncate_Table(db_engine, prefix):
    """清空指定数据表内容（保留表结构）"""
    sql_str = 'truncate table ' + prefix       # 生成清空表SQL语句
    with db_engine.connect() as conn:          # 使用连接上下文管理器
        conn.execute(text(sql_str))            # 执行SQL命令（使用text函数包装SQL）
        conn.commit()                          # 提交事务
    print(prefix, '数据表，已清空:', sql_str)    # 输出操作日志

def check_or_create_table(db_engine, prefix, dtype):
    """
    检查表是否存在，不存在则创建空表
    参数:
        db_engine: 数据库连接引擎
        prefix: 需要检查/创建的表名
        dtype: 表结构定义字典（字段类型映射）
    返回值:
        bool: True表示表已存在，False表示新创建了表
    """
    
    inspector = inspect(db_engine)
    
    if not inspector.has_table(prefix):
        # 创建空表结构
        # 根据dtype字典创建一个包含所有列的空DataFrame
        empty_df = pd.DataFrame({col: [] for col in dtype.keys()})
        empty_df.to_sql(
            name=prefix,
            con=db_engine,
            dtype=dtype,
            if_exists='replace'  # 确保不会覆盖现有表
        )
        print(f'已创建空表: {prefix}')
        return False
    return True

def get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime):
    """
    分页获取数据并写入数据库的核心逻辑
    Args:
        prefix:      接口名称（用于日志输出）
        db_engine:   数据库连接引擎
        ts_pro:      Tushare Pro接口对象
        get_data:    数据获取函数
        write_db:    数据写入函数
        rows_limit:  单次请求最大数据量
        times_limit: 接口每分钟调用限制
        sleeptime:   触发限制时的休眠时间
    """
    print(prefix, '接口：已调用：')
    itimes = 1    # 调用次数计数器
    offset = 0     # 分页偏移量
    
    while True:
        # 分页获取数据（核心数据获取逻辑）
        df = get_data(ts_pro, rows_limit, offset)
        print('df:',df)
        # 写入数据库（核心数据持久化逻辑）
        res = write_db(df, db_engine)
        print('res:',res)

        # 打印调用日志（包含调用次数/返回结果/数据日期/数据量）
        print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, '  数据日期:', currentDate, '  数据条数:', len(df))

        # 频率控制逻辑（达到调用上限时休眠）
        if itimes % times_limit == 0:
            isleeptime = sleeptime
            print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
            time.sleep(isleeptime)
        # 终止条件判断（数据量不足时退出循环）
        elif len(df) < rows_limit:
            # itimes = itimes + 1
            break  # 读不到了，就跳出去读下一个日期的数据
            
        offset = offset + rows_limit  # 翻页（增加偏移量）
        itimes = itimes + 1           # 调用次数递增

    return df

def get_and_write_data_by_code(db_engine, ts_pro, code,
                               get_data, write_db, prefix, times_limit, rows_limit):
    """
    按证券代码分页获取数据并写入数据库
    Args:
        code: 单个证券代码（如 '000001.SZ'）
        times_limit: 接口每分钟调用上限
        rows_limit: 单次请求最大数据量
        其他参数同 get_and_write_data_by_limit
    """
    itimes = 1    # 调用次数计数器
    offset = 0     # 分页偏移量
    
    while True:
        # 获取单页数据（传入证券代码和分页偏移）
        df = get_data(ts_pro, code, offset)
        # 写入数据库
        res = write_db(df, db_engine)
        
        # 打印带证券代码的调用日志
        print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, '  代码:', code, '  数据条数:', len(df))

        # 频率控制（达到调用上限休眠61秒）
        if itimes % times_limit == 0:
            isleeptime = 61
            print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
            time.sleep(isleeptime)
        # 终止条件（数据量不足时跳出循环）
        elif len(df) < rows_limit:
            itimes = itimes + 1
            break  # 读不到了，就跳出去读下一个日期的数据
            
        offset = offset + rows_limit  # 翻页操作
        itimes = itimes + 1          # 调用计数递增

    return df


def get_and_write_data_by_date(db_engine, ts_pro, market, start_date, end_date,
                               get_data, write_db, prefix, rows_limit, times_limit, sleeptime):
    """
    按日期范围分页获取证券数据并写入数据库
    
    该函数专为从Tushare Pro接口批量获取历史交易日数据设计，包含完整的：
    - 分页获取机制
    - 交易日验证功能
    - API调用频率控制
    - 数据库写入功能

    Args:
        db_engine (sqlalchemy.engine): 数据库连接引擎，需提前初始化
                                         示例：create_engine('postgresql://user:pass@localhost:5432/dbname')
        ts_pro (tushare.pro_api): 已初始化的Tushare Pro接口对象，需要有效的token
                                    示例：ts.pro_api('your_tushare_token')
        market (str): 交易所标识码，需大写字母组合
                        合法值：'SSE'（上交所）, 'SZSE'（深交所）, 'CFFEX'（中金所）等
        start_date (str): 开始日期，格式必须为YYYYMMDD
                            示例：'20230101'
        end_date (str): 结束日期（包含），格式必须为YYYYMMDD
                          示例：'20231231'
        get_data (function): 数据获取函数，需符合特定签名要求
                               函数签名示例：
                               def sample_get_data(ts_pro, date, offset, limit):
                                   return ts_pro.daily(trade_date=date, offset=offset, limit=limit)
        write_db (function): 数据写入函数，需返回None表示成功
                                函数签名示例：
                                def sample_write_db(df, engine):
                                    df.to_sql('table_name', engine, if_exists='append')
                                    return None
        prefix (str): 接口名称标识，用于日志输出（最大长度20字符）
                         示例：'每日行情'
        rows_limit (int): 单次API请求最大数据量（建议值：4000-5000）
                             Tushare Pro默认限制：5000条/次
        times_limit (int): 每分钟最大调用次数（根据Tushare权限级别设置）
                             基础权限：60次/分钟
                             高级权限：500次/分钟
        sleeptime (int): 触发调用限制时的冷却时间（单位：秒）
                           基础权限建议值：61（确保每分钟不超过60次）

    Returns:
        pandas.DataFrame: 最后获取的数据块，包含列名但可能为空（当日期范围内无数据时）

    Raises:
        ValueError: 当日期格式无效或start_date > end_date时
        ConnectionError: 数据库连接失败或Tushare接口不可用时

    注意事项：
        1. 日期范围包含start_date和end_date
        2. 会自动跳过非交易日（周末、节假日）
        3. 建议在非交易时段调用以避免接口拥堵
        4. 首次调用前需确保数据库表结构已创建
        5. 日志输出格式：[前缀] 接口：已调用：X次...
        6. 采用日期递增+分页双循环机制
        7. 每次切换日期时offset重置为0
    """
    print(prefix, '接口：已调用：传入日期： 开始日期：', start_date, '结束日期：', end_date)
    idate = start_date  # 当前处理日期
    predate = 10010101  # 前一个有效日期（初始化值）
    itimes = 1          # 接口调用计数器
    offset = 0          # 分页偏移量
    df = pd.DataFrame   # 空数据框初始化

    # 主循环：遍历日期范围
    while idate <= end_date:
        # 检查当前日期是否为交易日
        is_open = is_working_date(db_engine, market, idate)
        
        if is_open:  # 只在交易日处理数据
            # 分页获取数据循环
            while True:
                # 获取分页数据（传入日期和分页偏移）
                df = get_data(ts_pro, idate, offset, rows_limit)
                # 写入数据库
                res = write_db(df, db_engine)
                
                # 打印带日期的调用日志
                print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, ' 数据日期:', idate, '  数据条数:', len(df))

                # 频率控制（达到限制次数时休眠）
                if itimes % times_limit == 0:
                    isleeptime = sleeptime
                    print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
                    time.sleep(isleeptime)
                # 终止条件（数据量不足时跳出分页循环）
                elif len(df) < rows_limit:
                    itimes = itimes + 1
                    break  # 结束当前日期的数据获取( 读不到了，就跳出去读下一个日期的数据)
                    
                offset = offset + rows_limit  # 翻页
                itimes = itimes + 1          # 调用计数递增

            # 日期递增逻辑（+1天） 取下一日的数据
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            offset = 0  # 重置分页偏移量
            
        else:  # 非交易日处理
            # 直接跳到下一个日期
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            offset = 0
            continue  # 跳过后续处理

    return df


def get_and_write_data_by_codelist(db_engine, ts_pro, codeList, prefix,
                                   get_data, write_db,
                                   rows_limit, times_limit, sleeptimes):
    """
    批量处理证券代码列表的历史数据（支持分页抓取）
    
    专为需要遍历多个证券代码的场景设计，适用于：
    - 股票历史行情批量下载
    - 上市公司基本信息抓取
    - 财务报表数据批量获取

    Args:
        db_engine (sqlalchemy.engine): 数据库连接引擎（同日期版函数）
        ts_pro (tushare.pro_api): 已初始化的Tushare接口对象
        codeList (Iterable): 可迭代的证券代码集合，支持以下格式：
                            - pandas.Series: 如 pd.Series(['000001.SZ', '600000.SH'])
                            - numpy.ndarray: 如 np.array(['000001.SZ', '600000.SH'])
                            - 列表: ['000001.SZ', '600000.SH']
        prefix (str): 接口标识（最大15字符），用于日志输出
                     示例：'个股基本信息'
        get_data (function): 自定义数据获取函数，需满足签名：
                            def func(ts_pro, code, offset):
                               返回 pandas.DataFrame
                           示例：
                           def stock_basic(ts_pro, code, offset):
                               return ts_pro.stock_basic(ts_code=code, offset=offset*5000, limit=5000)
        write_db (function): 数据写入函数（同日期版函数）
        rows_limit (int): 单次请求最大数据量（500-5000）
                         根据Tushare接口限制设置
        times_limit (int): 每分钟最大调用次数（基础版60/分钟，高级版500/分钟）
        sleeptimes (int): 冷却时间（基础版建议61秒，高级版建议10秒）

    Returns:
        pandas.DataFrame: 最后获取的数据块（用于错误检查）

    Raises:
        TypeError: 当codeList包含非字符串元素时
        ConnectionAbortedError: 数据库连接中断时

    调用示例：
        // 准备代码列表（沪深300成分股）
        codes = ['600519.SH', '000858.SZ', '300750.SZ'] 
        
        // 定义数据获取函数（示例：获取日线数据）
        def fetch_daily(ts_pro, code, offset):
            return ts_pro.daily(ts_code=code, 
                              offset=offset*5000, 
                              limit=5000)
            
        // 执行批量抓取
        result = get_and_write_data_by_codelist(
            db_engine=engine,
            ts_pro=ts_pro,
            codeList=codes,
            prefix='个股日线',
            get_data=fetch_daily,
            write_db=save_to_db,
            rows_limit=5000,
            times_limit=60,
            sleeptimes=61
        )

    日志输出示例：
        个股日线 接口：已调用，调用时间：2023-10-01 03:00:01 
        当前：1/500次,代码：600519.SH，写入数据库返回(None表示成功): None   
        数据条数: 5000
        个股日线 接口：已调用60次，sleep 61s
    """
    itimes = 1           # 总调用次数计数器
    iTotal = codeList.__len__()  # 获取代码列表总长度
    df = pd.DataFrame    # 初始化空数据框
    codeListArray = codeList.__array__()  # 将输入转换为数组格式
    # print('codeListArray:', codeListArray)
    # 遍历代码列表
    for code in codeListArray:
        offset = 0  # 重置分页偏移量
        # 单个代码的分页循环
        while True:
            # 将数组格式转化为字符串格式
            code = str(code[0])
            print('code:', code)
            # 获取单页数据（传入当前代码和偏移量）
            df = get_data(ts_pro, code, offset)
            # 写入数据库
            res = write_db(df, db_engine)
            
            # 打印带时间戳的详细日志
            print(prefix, '接口：已调用，调用时间：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '当前：', itimes, '/', codeList.__len__(), '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                  '  数据条数:', len(df))

            # 接口调用频率控制
            if itimes % times_limit == 0:
                isleeptime = sleeptimes
                print(prefix, '接口：已调用', times_limit, '次，根据该接口限制，sleep ', sleeptimes, 's，后再继续调用！')
                time.sleep(isleeptime)
            # 终止条件判断（数据不足一页时跳出）
            elif len(df) < rows_limit:
                itimes = itimes + 1
                break  # 结束当前代码的数据获取 读不到了，就跳出去读下一个日期的数据
                
            offset = offset + rows_limit  # 翻页
            itimes = itimes + 1          # 总调用次数递增

    return df

def get_and_write_data_by_long_codelist(db_engine, ts_pro, codeList, prefix,
                                        get_data, write_db, codes_onetime,
                                        rows_limit, times_limit, sleeptimes):
    """
    批量处理超长证券代码列表的优化抓取方案（支持代码分组+分页）
    Args:
        codes_onetime: 单次API调用最大代码承载量（一次调用最多获取多少个代码对应的数据，针对可以一次传多个代码的接口的快速取数）
        codeList: 证券代码集合（支持DataFrame/Series/List）
        其他参数同 get_and_write_data_by_codelist
    """
    # codes_onetime ：一次调用最多获取多少个代码对应的数据，针对可以一次传多个代码的接口的快速取数
    itimes = 1  # 第几次调用
    i = 1  # 循环了几次
    codelist_len = codeList.__len__()
    codeListArray = codeList.__array__()
    codes = ''
    offset = 0
    df = pd.DataFrame
    for code in codeListArray:
        if codes == '':
            codes = code[0]
        else:
            codes = codes + ',' + code[0]
            # print('codes:', codes)
        if i % codes_onetime == 0 or i + 1 == codelist_len:
            # 对于每次代码列表的调用，按照该接口单次代码的限制进行多次取数据
            offset = 0
            while True:
                df = get_data(ts_pro, codes, rows_limit, offset)
                res = write_db(df, db_engine)

                print(prefix, '接口：已调用：', itimes,
                      '次，返回结果(None表示成功):', res, '  数据日期:', currentDate, '  数据条数:', len(df))

                if itimes % times_limit == 0:
                    print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', sleeptimes, 's')
                    time.sleep(sleeptimes)
                elif len(df) < rows_limit:
                    itimes = itimes + 1
                    break  # 读不到了，就跳出去读下一个日期的数据
                offset = offset + rows_limit
                itimes = itimes + 1
            codes = ''  # 本次证券列表用完后，清空，以便下一次调用时不会重复调用

        i = i + 1

    return df

def get_and_write_data_by_date_and_codelist(db_engine, ts_pro, prefix, times_limit, sleeptimes,
                                            get_data, write_db, codeList, str_date, end_date):
    """
    按日期范围+证券代码列表的组合方式获取数据（双重循环遍历）
    Args:
        str_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（包含）
        codeList: 证券代码集合（支持DataFrame/Series/List）
        其他参数同 get_and_write_data_by_codelist

    Returns:
        pandas.DataFrame: 最后获取的数据块（用于错误检查）

    #### 处理逻辑示意图
    for 每个日期 in 开始日期到结束日期:
        if 是交易日:
            for 每个代码 in 代码列表:
                获取该代码当日数据 -> 写入数据库
                控制调用频率
        else:
            跳过该日期
    """
    idate = str_date  # 初始化当前处理日期
    iTimes = 0        # 接口总调用次数
    iTotal = codeList.__len__()  # 代码总数
    codeListArray = codeList.__array__()  # 转换为可迭代数组
    df = pd.DataFrame  # 空数据框初始化

    # 主循环：遍历日期范围
    while idate <= end_date:
        # 交易日验证（需确保is_working_date参数正确性）
        is_open = is_working_date(db_engine, idate)
        
        if is_open:  # 交易日处理逻辑
            # 遍历当前日期下的所有代码
            for code in codeListArray:
                # 获取单个代码当日数据
                df = get_data(ts_pro, code, idate)
                # 写入数据库
                res = write_db(df, db_engine)
                # 更新调用计数器
                iTimes = iTimes + 1

                # 打印带日期+代码的复合日志
                print(prefix, '接口：已调用，读取日期为', idate, '的数据， ', iTimes, '/',
                      codeList.__len__(), '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                      '  抓取数据条数:', len(df), time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))

                # 频率控制（达到限制次数时休眠）
                if iTimes % times_limit == 0:
                    print(prefix, '接口：已调用', times_limit, '次，根据该接口限制，sleep ', sleeptimes, 's，后再继续调用！')
                    time.sleep(sleeptimes)

            # 取下一日
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
        else:  # 非交易日处理
            # 直接递增日期
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            continue  # 跳过代码遍历

    return df


def get_and_write_data_by_start_end_date_and_codelist(db_engine, ts_pro, prefix, get_data, write_db, times_limit,
                                                      sleeptimes, rows_limit, codeList, str_date_iso, end_date_iso):
    """
    按日期范围+证券代码列表的组合方式获取历史数据（支持分页）
    Args:
        str_date_iso: 开始日期（格式：YYYYMMDD）
        end_date_iso: 结束日期（包含）
        codeList: 证券代码集合（支持DataFrame/Series/List）
        rows_limit: 单次请求最大数据量（根据接口限制设置）
        其他参数同 get_and_write_data_by_codelist

    注意：
    1. 该函数主要用于获取历史数据，不支持实时数据获取。
    2. 该函数适用于一次性获取大量历史数据的场景，不适用于实时数据获取。

    #### 三维处理逻辑（日期范围 + 证券代码 + 分页）
    for 每个代码 in 代码列表:
        while True:  # 分页循环
            获取该代码在[start_date, end_date]日期范围内的数据（第offset页）
            if 数据量不足一页:
                break  # 该代码数据已取完
            写入数据库
            控制接口调用频率
            
            if 达到每分钟调用限制:
                sleep(冷却时间)  # 如：基础版每分钟60次调用后休眠61秒
    """
    print(prefix, '接口：已调用：传入日期： 开始日期：', str_date_iso, '结束日期：', end_date_iso)
    iTimes = 1    # 接口总调用次数
    iCode = 1     # 代码遍历进度计数器
    iTotal = codeList.__len__()  # 代码总数
    codeListArray = codeList.__array__()  # 转换为可迭代数组
    df = pd.DataFrame  # 空数据框初始化

    # 遍历代码列表
    for code in codeListArray:
        offset = 0  # 分页偏移量重置
        # 分页获取当前代码的历史数据
        while True:
            # 获取指定日期范围内的分页数据（带代码和分页参数）
            df = get_data(ts_pro, code, offset, str_date_iso, end_date_iso)
            # 写入数据库
            res = write_db(df, db_engine)

            # 打印带时间戳的复合日志
            print(prefix, '接口：已调用，调用时间：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '读取日期从:', str_date_iso, '到 ', end_date_iso, '的数据， ', iCode, '/',
                  iTotal, '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                  '  抓取数据条数:', len(df))
            
            # 频率控制（达到限制次数时休眠）
            if iTimes % times_limit == 0:
                isleeptime = sleeptimes
                print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
                time.sleep(isleeptime)
            # 终止条件（数据量不足时结束当前代码）
            elif len(df) < rows_limit:
                iTimes = iTimes + 1
                iCode = iCode + 1
                break  # 退出分页循环，处理下一代码
                
            offset = offset + rows_limit  # 翻页
            iTimes = iTimes + 1          # 总调用次数更新

    return df


#按证券代码获取历史数据（不分页）
def get_and_write_data_by_code_simple(db_engine, ts_pro, codeList, prefix,
                                   get_data, write_db, times_limit, sleeptimes):
    """
    按证券代码获取历史数据（不分页版本）
    Args:
        db_engine: 数据库连接引擎
        ts_pro: Tushare Pro接口对象
        codeList: 证券代码集合（支持DataFrame/Series/List）
        prefix: 接口标识，用于日志输出
        get_data: 数据获取函数，需满足签名：def func(ts_pro, code) -> DataFrame
        write_db: 数据写入函数，需满足签名：def func(df, engine) -> None
        times_limit: 每分钟最大调用次数
        sleeptimes: 触发调用限制时的休眠时间（秒）
    Returns:
        pandas.DataFrame: 最后获取的数据块
    """
    itimes = 1           # 总调用次数计数器
    iTotal = codeList.__len__()  # 获取代码列表总长度
    df = pd.DataFrame    # 初始化空数据框
    codeListArray = codeList.__array__()  # 将输入转换为数组格式

    # 遍历代码列表
    for code in codeListArray:
        # 将数组格式转化为字符串格式
        code = str(code[0])
        print('code:', code)
        
        # 获取该代码的全量数据
        df = get_data(ts_pro, code)
        # 写入数据库
        res = write_db(df, db_engine)
        
        # 打印带时间戳的详细日志
        print(prefix, '接口：已调用，调用时间：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '当前：', itimes, '/', iTotal, '次,代码：', code, '，写入数据库返回(None表示成功):', res,
              '  数据条数:', len(df))

        # 接口调用频率控制
        if itimes % times_limit == 0:
            print(prefix, '接口：已调用', times_limit, '次，根据该接口限制，sleep ', sleeptimes, 's，后再继续调用！')
            time.sleep(sleeptimes)
            
        itimes = itimes + 1  # 调用次数递增

    return df