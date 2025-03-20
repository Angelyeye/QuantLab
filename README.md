# QuantLab 量化交易实验室

一个基于Python的量化交易系统，集成了数据获取、策略回测和自动化交易等功能。专为初学者设计，无需深厚的编程基础即可上手使用。

## 目录

- [功能特性](#功能特性)
- [新手入门：什么是量化交易？](#新手入门什么是量化交易)
- [环境要求](#环境要求)
- [安装指南](#安装指南)
  - [Python安装](#python安装)
  - [项目安装](#项目安装)
- [配置说明](#配置说明)
  - [Tushare API注册](#tushare-api注册)
  - [数据库配置](#数据库配置)
- [使用教程](#使用教程)
  - [数据获取](#数据获取)
  - [策略回测](#策略回测)
  - [常见问题](#常见问题)
- [项目结构](#项目结构)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 功能特性

- **数据获取模块**
  - 支持从Tushare获取股票、基金、可转债等多种金融数据
  - 自动检测和补全缺失数据
  - 支持实时和历史数据的获取和更新
  - 多种数据类型：日线、分钟线、基本面、财务数据等

- **策略回测**
  - 内置多种技术指标和策略模板
  - 支持自定义交易策略
  - 提供完整的回测分析报告（收益率、夏普比率、最大回撤等）
  - 可视化回测结果

## 新手入门：什么是量化交易？

量化交易是利用计算机技术和数学模型，根据历史数据制定交易策略并自动执行的交易方式。简单来说，就是用数据和程序来辅助投资决策，而不是凭感觉买卖。

**基本流程：**

1. **获取数据**：收集股票、基金等金融产品的历史价格、交易量等数据
2. **策略设计**：根据技术指标（如均线、MACD等）设计买卖规则
3. **回测验证**：用历史数据测试策略效果
4. **实盘交易**：将验证有效的策略应用到实际交易中

本系统帮您简化了上述流程，即使没有编程基础，也能轻松开始量化交易之旅。

## 环境要求

- **Python 3.7+**：编程语言环境
- **依赖包**：
  - pandas：数据处理
  - sqlalchemy：数据库操作
  - tushare：金融数据接口
  - backtrader：回测框架
  - retry：网络请求重试
  - pathlib：路径处理

## 安装指南

### Python安装

如果您还没有安装Python，请按照以下步骤操作：

1. 访问[Python官网](https://www.python.org/downloads/)下载最新版Python
2. 运行安装程序，**务必勾选「Add Python to PATH」选项**
3. 完成安装后，打开命令提示符（按Win+R，输入cmd），输入以下命令验证安装：
   ```
   python --version
   ```
   如果显示Python版本号，则安装成功

### 项目安装

1. **下载项目**
   - 方法一：使用Git（推荐）
     ```bash
     git clone [your-repository-url]
     cd QuantLab
     ```
   - 方法二：直接下载ZIP压缩包并解压

2. **安装依赖**
   - 打开命令提示符，进入项目目录
   - 执行以下命令安装所需的所有依赖包：
     ```bash
     pip install -r requirements.txt
     ```
     此命令会自动安装requirements.txt文件中列出的所有依赖包

## 配置说明

### Tushare API注册

1. 访问[Tushare官网](https://tushare.pro/)注册账号
2. 登录后，在个人中心页面找到并复制您的API Token

### 数据库配置

1. 复制配置模板文件：
   ```bash
   # Windows系统
   copy config.example.ini config.ini
   
   # Linux/Mac系统
   cp config.example.ini config.ini
   ```

2. 使用文本编辑器（如记事本）打开`config.ini`文件

3. 填写配置信息：
   ```ini
   [tushare]
   # 将your_tushare_token_here替换为您在Tushare获取的Token
   token = your_tushare_token_here
   
   [database]
   # 数据库连接信息
   # 格式：mysql+pymysql://用户名:密码@主机地址:端口/数据库名
   connection = mysql+pymysql://username:password@localhost:3306/database_name
   ```

4. 保存文件

## 使用教程

### 数据获取

#### 1. 获取每日数据

运行以下命令获取最新的每日行情数据：

```bash
python getData/get_EveryDayData.py
```

或者在Python代码中调用：

```python
from getData import get_EveryDayData

# 执行每日数据更新
get_EveryDayData.get_everyday_data()
```

#### 2. 处理缺失数据

如果您发现数据有缺失，可以运行以下命令补全：

```python
from getData import deal_LostData

# 检查并补全缺失数据
deal_LostData.deal_lost_data()
```

#### 3. 按股票代码获取缺失数据

```python
from getData import get_LostData_By_Code

# 补全特定股票的缺失数据，例如：'000001.SZ'（平安银行）
get_LostData_By_Code.get_lost_data_by_code('000001.SZ')
```

### 策略回测

以下是使用内置的简单移动平均线(SMA)策略进行回测的示例：

```python
# 导入策略模块
from QuantStrategies import bt_strategy_sma_tushare2db

# 运行回测
# 该模块会自动从数据库获取数据，执行回测，并显示回测结果
# 默认回测贵州茅台(600519.SH)的数据
bt_strategy_sma_tushare2db
```

回测结果将包括：
- 交易记录
- 收益率分析
- 年化收益率
- 夏普比率
- 最大回撤
- 可视化图表

### 常见问题

#### 1. 数据获取失败

**问题**：运行数据获取脚本时出现错误

**解决方案**：
- 检查网络连接
- 确认Tushare API Token是否正确
- 检查是否超出了Tushare的API调用限制

#### 2. 数据库连接错误

**问题**：无法连接到数据库

**解决方案**：
- 确认数据库服务是否启动
- 检查config.ini中的数据库连接信息是否正确
- 确认数据库用户是否有足够权限

#### 3. 回测结果不显示图表

**问题**：回测完成但没有显示图表

**解决方案**：
- 确认已安装matplotlib库：`pip install matplotlib`
- 检查是否在支持图形界面的环境中运行

## 项目结构

```
├── basis/                 # 基础工具和配置
│   ├── Init_Env.py        # 环境初始化
│   ├── Tools.py           # 工具函数
│   └── constant.py        # 常量定义
├── getData/               # 数据获取模块
│   ├── get_EveryDayData.py    # 每日数据更新
│   ├── deal_LostData.py       # 缺失数据处理
│   ├── get_LostData_By_Code.py # 按股票代码获取缺失数据
│   └── get_LostData_By_Date.py # 按日期获取缺失数据
├── getDataFromTushare/    # Tushare数据接口
│   ├── Get_Stock_Daily_ToDB.py # 获取股票日线数据
│   ├── Get_Fund_Daily_ToDB.py  # 获取基金日线数据
│   └── ...                # 其他数据获取接口
├── QuantStrategies/       # 量化策略模块
│   └── bt_strategy_sma_tushare2db.py # 简单移动平均线策略
├── config.example.ini     # 配置文件模板
└── requirements.txt       # 项目依赖
```

## 贡献指南

我们欢迎所有形式的贡献，无论是新功能、文档改进还是bug修复。

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 许可证

[MIT](LICENSE)