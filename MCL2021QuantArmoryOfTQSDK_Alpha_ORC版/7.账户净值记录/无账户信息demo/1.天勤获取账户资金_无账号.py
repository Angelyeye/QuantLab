from tqsdk import TqApi, TqAuth, TqKq, TqAccount

TQAuth = '天勤账号' + ',' + '天勤密码'
api = TqApi(TqKq(), auth=TQAuth)
# api = TqApi(TqAccount(FutureCompany, FutureAccount, FuturePassword), auth=TQAuth)
account = api.get_account()
MyMoneyNow = account.balance
print('当前账户余额:' + str(MyMoneyNow))
api.close()