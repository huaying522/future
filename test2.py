
import rqdatac as rq
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker




# 数据
# rq.init('license', 'baGMRBMmIyaNAKydGsjT3faSYO0hS_aOUc-1JT7MmKC1mAfZ54ZVIXUkkv1YWQvBvPSPHoCqxfF2kIMALgRykNRYmi98i4DzDkPjxtkdxs7xk52tlxQvosUVig17oO2xg3vxC_lmogBBywTV4ypjLDfiHbfhaBeaPxpH-cWR3XQ=PYcbJmPh6ERfrXybPPbt1lHeLygc2FwUDjSoSAeK-tOj0IwAYbLVUj6T7DEWsknp11lWGCXXZR0D1bWtFJjCsy2jZJ15NB7OnoXQTgZdlydkprQxdm3E0UHDHrii-EHzUYaYKaHJB5FxaEqq6J069mAR-6w9hDDL1Ha8E13VoL4=')
rq.init('17396110410', 'haha12345')
pzs = ['IF']

for pz in pzs:
    print("当前下载品种：{0}".format(pz))
    order_book_ids = rq.futures.get_contracts(pz[0])
    print("当前下载合约：{0}".format(",".join(order_book_ids)))
    data = rq.get_price(order_book_ids, start_date='2023-05-15', end_date='2023-05-20', frequency='1m', fields=None, adjust_type='pre', skip_suspended=False, market='cn', expect_df=True)
    # data = rq.all_instruments(type='Future', date='20230821')
    # data = rq.all_instruments(type='Future')
    # print(data)
    data['period'] = '1m'
    data['trading_date'] = data['trading_date'].astype(str).str.slice(0, 10)
    data = data.reindex(columns=['period', 'open', 'trading_date', 'volume', 'low', 'total_turnover',
       'open_interest', 'close', 'high'])
    print(data.columns)
    print(data.index)
    print(data.index)
    print(data.index, "modified")
    # data.to_csv('output.csv', index=True)
    # 插入数据
    # data.to_sql('dd_future_quota_rq', engine, index=True, if_exists='append')



# 关闭连接
# session.close()
# conn.close()



