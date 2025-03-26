# 引入TqSdk模块
from tqsdk import TqApi, TqAuth

# 品种
Symbol = 'SHFE.rb2110'
Period = 60
# 创建api实例，设置web_gui=True生成图形化界面
api = TqApi(web_gui=True, auth=TqAuth("信易账户", "账户密码"))
# 订阅 K线
klines = api.get_kline_serial(Symbol, Period)
while True:
    # 通过wait_update刷新数据
    api.wait_update()