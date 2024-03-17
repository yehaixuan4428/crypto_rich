# -*- coding: UTF-8 -*-
'''
@Project    : crypto 
@File       : test_binance_api.py
@Author     : Haixuan Ye (haixuanye@outlook.com)
@Date       : 2024/2/26 22:08 
@Description: 
'''
from binance.spot import Spot
from datetime import datetime


proxies = { 'https': '127.0.0.1:7890' }
client= Spot(proxies=proxies)

# Get server timestamp
print(client.time())
# # Get klines of BTCUSDT at 1m interval
# print(client.klines("BTCUSDT", "1m"))
# Get last 10 klines of BNBUSDT at 1h interval
start_time = int(datetime(2024, 2, 20).timestamp()) * 1000
end_time = int(datetime(2024, 2, 20, hour = 23, minute=59, second=59).timestamp()) * 1000
kline = client.klines("BTCUSDT", "1m", startTime = start_time, endTime=end_time, limit = 1000)
print(datetime.fromtimestamp(kline[0][0]/1e3))
print(datetime.fromtimestamp(kline[0][6]/1e3))

print(datetime.fromtimestamp(kline[1][0]/1e3))
print(datetime.fromtimestamp(kline[1][6]/1e3))
# get all symbols
# symbols = client.exchange_info()['symbols']

# API key/secret are required for user data endpoints
# api_key = '7o2ksXYuJ18XYzIhVHou0sj9fZeIN8iSCy4wuNc91yGYC4xM2YZo0pCcpa8FN2xO'
# api_secret = 'I8yNntw2idqgEThnQGtibXoJla10v3L3ndr4LKUbAXQS3vH1MEN0m6te3XsHW1qN'
# client = Spot(api_key=api_key, api_secret=api_secret, proxies=proxies)
#
# # Get account and balance information
# print(client.account())
