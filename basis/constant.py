# 常量定义，如API Token和数据库连接信息

class constant:
    """单例模式常量管理类，通过重写__setattr__实现常量保护机制
    
    内部类ConstError用于在尝试修改已定义常量时抛出特定异常
    """
    class ConstError(TypeError):
        """自定义异常类型，当尝试修改已定义的常量时触发"""
        pass

    def __setattr__(self, name, value):
        """属性设置拦截方法，实现常量不可变性
        
        参数:
        name (str): 尝试设置的属性名
        value (any): 尝试设置的属性值
        
        异常:
        ConstError: 当尝试修改已存在的常量时抛出
        """
        # 检查属性是否已经存在
        if name in self.__dict__:
            # 如果属性已存在，抛出常量错误
            raise self.ConstError("Can't rebind const (%s)" % name)
        # 如果属性不存在，将其添加到实例的__dict__中
        self.__dict__[name] = value


import configparser
import os

# 读取配置文件
config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')

# 检查配置文件是否存在
if not os.path.exists(config_file):
    example_config = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.example.ini')
    raise FileNotFoundError(
        f'配置文件 {config_file} 不存在。\n'
        f'请根据示例配置文件 {example_config} 创建配置文件。'
    )

config.read(config_file, encoding='utf-8')

# 初始化常量
constant = constant()
constant.proToken = config.get('tushare', 'token')
constant.dbPath = config.get('database', 'connection')


def get_tushare_token() -> str:
    """获取Tushare Pro API认证令牌
    
    从全局常量配置中读取并返回预先设置的Tushare Pro API Token
    
    Returns:
        str: 用于接口认证的令牌字符串，格式通常为32位哈希值
        
    Example:
        >>> token = get_tushare_token()
        >>> len(token)
        32
        
    Note:
        该Token需在constant.py中配置，使用时请注意保密
    """
    return constant.proToken


def get_db_path() -> str:
    """获取数据库连接配置信息。

    从全局常量配置中读取并返回预先设置的数据库连接路径，该路径使用SQLAlchemy URI格式：
    mysql+pymysql://<username>:<password>@<host>:<port>/<database>

    Returns:
        str: 数据库连接字符串，包含认证信息和连接参数。

    Example:
        >>> db_uri = get_db_path()
        >>> print(db_uri)
        'mysql+pymysql://root:030201@localhost:3306/QTdatabase'

    Note:
        连接字符串包含敏感信息，请勿直接暴露在日志或前端页面。
    """
    return constant.dbPath