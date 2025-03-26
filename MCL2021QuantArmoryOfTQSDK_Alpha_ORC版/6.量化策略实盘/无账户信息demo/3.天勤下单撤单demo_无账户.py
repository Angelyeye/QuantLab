from tqsdk import TqApi, TqAuth, TqKq, TqAccount

TQAuth = '天勤账号' + ',' + '天勤密码'
api = TqApi(TqKq(), auth=TQAuth)
# api = TqApi(TqAccount(FutureCompany, FutureAccount, FuturePassword), auth=TQAuth)
TradeSymbolName = 'SHFE.rb2201'
Quote = api.get_quote(TradeSymbolName)
Times = 5
i = 1
while True:
	api.wait_update()
	bidprice1 = Quote.bid_price1
	askprice1 = Quote.ask_price1
	if i <= Times:
		order = api.insert_order(symbol=TradeSymbolName, direction="BUY", offset="OPEN", volume=1, limit_price=bidprice1)
		api.cancel_order(order)
	i += 1