from __future__ import absolute_import, division, print_function, unicode_literals

import datetime  # For datetime objects
import backtrader as bt  # Import the backtrader platform
import pandas as pd


class GenericDdbData(bt.feeds.PandasData):

    # ddb_session = BinanceTools.create_ddb_client()
    # db_path = "dfs://crypto_kline"
    # table_name = "kline_1min"

    # table = ddb_session.loadTable(tableName=table_name, dbPath=db_path)
    # data = table.select("*").where("symbol='BTCUSDT'").limit(5).toDF()
    # data = GenericDdbData(
    #     dataname=data,
    #     datetime="open_time",
    #     openinterest=None,
    # )

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


def getWinLoss(analyzer):
    return analyzer.won.total, analyzer.lost.total, analyzer.pnl.net.total


def getSQN(analyzer):
    return round(analyzer.sqn, 2)


def run_backtest(
    data: pd.DataFrame,
    strategy,
    **kwargs,
):
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a FixedSize sizer according to the stake
    if kwargs.get("stake_val"):
        cerebro.addsizer(
            bt.sizers.FixedSize, stake=kwargs.get("stake_val")
        )  # Multiply the stake by X

    initial_cash_value = kwargs.get("portfolio")
    cerebro.broker.setcash(initial_cash_value)

    if kwargs.get("commission_val"):
        cerebro.broker.setcommission(commission=kwargs.get("commission_val") / 100)

    kwargs.pop("stake_val", None)
    kwargs.pop("portfolio", None)
    kwargs.pop("commission_val", None)

    # Add a strategy
    cerebro.addstrategy(strategy, **kwargs)

    # Add the Data Feed to Cerebro
    data = GenericDdbData(
        dataname=data,
        datetime="open_time",
        openinterest=None,
        timeframe=bt.TimeFrame.Minutes,
        name="BTCUSDT",
    )
    # cerebro.adddata(data)

    cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=1440)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addanalyzer(bt.analyzers.SQN, _name="sqn")

    #    try:     # convenience try/exception block
    strat = cerebro.run()
    stratexe = strat[0]

    try:
        totalwin, totalloss, pnl_net = getWinLoss(stratexe.analyzers.ta.get_analysis())
    except KeyError:
        totalwin, totalloss, pnl_net = 0, 0, 0

    sqn = getSQN(stratexe.analyzers.sqn.get_analysis())

    start_portfolio = initial_cash_value
    profit = (pnl_net / start_portfolio) * 100
    end_val = cerebro.broker.getvalue()

    # view the data in the console while processing
    print(
        "Net $%.2f (%.2f%%) WL %d/%d SQN %.2f"
        % (
            end_val - start_portfolio,
            (end_val - start_portfolio) / start_portfolio * 100,
            totalwin,
            totalloss,
            sqn,
        )
    )

    if kwargs.get("plot", False):
        cerebro.plot()

    return cerebro.broker.getvalue(), totalwin, totalloss, pnl_net, sqn


#   except Exception as e:         # handle unexpected errors gracefully
#       print('Error:', str(e))
#       return 0, 0, 0, 0, 0
