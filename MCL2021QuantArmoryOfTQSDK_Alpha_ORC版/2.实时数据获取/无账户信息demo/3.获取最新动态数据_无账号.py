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
# 进入循环更新最新数据
while True:
    api.wait_update()
    print(' ====================== 新数据 ====================== ')
    print('quote:')
    print(quote)
    print('ticks:')
    print(ticks)
    print('klines:')
    print(klines)