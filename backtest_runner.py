import pandas as pd
from backtrader_engine import run_backtest, getWinLoss, getSQN
from bt_strategies.double_moving_ma import CrossOverStrategy
from bt_strategies.RSIStrategy import RSIStrategy
from binance_tools import BinanceTools


if __name__ == "__main__":
    commission_val = 0.0075  # spot% taker fees binance usdt futures
    start_portfolio = 100000.0
    portfolio = start_portfolio
    stake_val = 1.0

    ### strategy parameters
    strategy = RSIStrategy
    quantity = 0.10
    period = 12
    stopLoss = 0
    limits = [70, 30]
    plot = True

    start_date = pd.to_datetime("20240301")
    end_date = pd.to_datetime("20240310")

    ddb_client = BinanceTools.create_ddb_client()
    t = ddb_client.loadTable(dbPath="dfs://crypto_kline", tableName="kline_1min")

    data = (
        t.select("*")
        .where("symbol='BTCUSDT'")
        .where(f"date(open_time)>= {start_date.strftime('%Y.%m.%d')}")
        .where(f"date(open_time)<= {end_date.strftime('%Y.%m.%d')}")
        .toDF()
    )
    print(data)

    end_val, totalwin, totalloss, pnl_net, sqn = run_backtest(
        strategy=strategy,
        data=data,
        maperiod=period,
        quantity=quantity,
        upper=limits[0],
        lower=limits[1],
        stopLoss=stopLoss,
        commission_val=commission_val,
        portfolio=portfolio,
        stake_val=stake_val,
        plot=plot,
    )

    profit = (pnl_net / portfolio) * 100

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
