from binance_tools import BinanceTools
import datetime
import backtrader as bt
import numpy as np

ddb_session = BinanceTools.create_ddb_client()
db_path = "dfs://crypto_kline"
table_name = "kline_1min"

table = ddb_session.loadTable(tableName=table_name, dbPath=db_path)
data = table.select("*").where("symbol='BTCUSDT'").limit(5).toDF()
print(data.columns)
print(data)


class GenericDDB(bt.feeds.PandasData):

    # Add a 'pe' line to the inherited ones from the base class
    lines = (
        # "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    )

    # openinterest in GenericCSVData has index 7 ... add 1
    # add the parameter to the parameters inherited from the base class
    params = (
        # ("close_time", 6),
        ("quote_asset_volume", 7),
        ("number_of_trades", 8),
        ("taker_buy_base_asset_volume", 9),
        ("taker_buy_quote_asset_volume", 10),
    )


data = GenericDDB(
    dataname=data,
    datetime="open_time",
    openinterest=None,
)


class TestStrategy(bt.Strategy):
    def __init__(self):
        pass

    def next(self):
        print(self.data.number_of_trades[0])


cerebro = bt.Cerebro()
cerebro.addstrategy(TestStrategy)
cerebro.adddata(data)
cerebro.run()
