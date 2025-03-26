from tqsdk import TqApi, TqAuth
import pandas as pd

# 品种
Symbol = 'SHFE.rb2110'
# 创建API实例,传入自己的信易账户
api = TqApi(auth=TqAuth("信易账户", "账户密码"))
# 获取quote
quote = api.get_quote(Symbol)
# 获取tick
ticks = api.get_tick_serial(Symbol)
# 获取K线
klines = api.get_kline_serial(Symbol, 60, 500)
# 查看结果
print(quote)
print(ticks)
print(klines)
# 落地tick
ticks.to_csv('tick数据.csv', index = 0)
# 落地K线
klines.to_csv('klines数据.csv', index = 0)
# 关闭api,释放资源
api.close()