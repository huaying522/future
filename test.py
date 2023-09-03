
import rqdatac as rq
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# 连接数据库
engine = create_engine('mysql+pymysql://root:000000@localhost:3306/quant_invest')

sql = "select distinct underlying_symbol from quant_invest.dd_future_baseinfo_rq"
# sql = "select distinct  underlying_symbol from quant_invest.dd_future_baseinfo_rq where underlying_symbol = 'IF'"
df_pzs = pd.read_sql_query(text(sql), engine.connect())
pzs = df_pzs.values.tolist()
print(pzs)
# pzs = ['RI','PM','RM','RB','WR','CF','SR','AG','WH','MA','OI','JR','SM','FG','HC','LR','A','SF','CS','M','B','C','WT','RO','ER','ME','S','RS','TC','CY','AP','SC','TS','SP','EG','CJ','RR','UR','NR','SS','EB','SA','PG','LU','PF','BC','LH','PK','IM','SI','TL','AO','LC','BR','EC']

# 数据
# rq.init('license', 'baGMRBMmIyaNAKydGsjT3faSYO0hS_aOUc-1JT7MmKC1mAfZ54ZVIXUkkv1YWQvBvPSPHoCqxfF2kIMALgRykNRYmi98i4DzDkPjxtkdxs7xk52tlxQvosUVig17oO2xg3vxC_lmogBBywTV4ypjLDfiHbfhaBeaPxpH-cWR3XQ=PYcbJmPh6ERfrXybPPbt1lHeLygc2FwUDjSoSAeK-tOj0IwAYbLVUj6T7DEWsknp11lWGCXXZR0D1bWtFJjCsy2jZJ15NB7OnoXQTgZdlydkprQxdm3E0UHDHrii-EHzUYaYKaHJB5FxaEqq6J069mAR-6w9hDDL1Ha8E13VoL4=')
rq.init('17396110410', 'haha12345')
#
# 下载参数
startdate = '2023-07-31'
enddate = '2023-08-31'
s = sessionmaker(engine)
session = s()
del_sql = "delete from quant_invest.dd_future_quota_rq where trading_date between '{0}' and '{1}'".format(startdate, enddate)
print(del_sql)
session.execute(text(del_sql))
session.commit()

for pz in pzs:
    print("当前下载品种：{0}".format(pz))
    try:
        order_book_ids = rq.futures.get_contracts(pz[0])
        print("当前下载合约：{0}".format(",".join(order_book_ids)))
        data = rq.get_price(order_book_ids, start_date=startdate, end_date=enddate, frequency='1m', fields=None, adjust_type='pre', skip_suspended=False, market='cn', expect_df=True)
        # data = rq.all_instruments(type='Future', date='20230821')
        # data = rq.all_instruments(type='Future')
        # print(data)
        data['period'] = '1m'
        data['trading_date'] = data['trading_date'].astype(str).str.slice(0, 10)
        data = data.reindex(columns=['period', 'open', 'trading_date', 'volume', 'low', 'total_turnover',
           'open_interest', 'close', 'high'])
        print(data.columns)
        print(data.index)
        # data.to_csv('output.csv', index=True)
        # 插入数据
        data.to_sql('dd_future_quota_rq', engine, index=True, if_exists='append')
    except:
        print("品种{0}没有可交易合约！".format(pz))
        pass



# 关闭连接
# session.close()
# conn.close()



