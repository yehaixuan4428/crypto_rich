from binance_tools import BinanceTools
import datetime
import sys

if __name__ == "__main__":
    start_dt = sys.argv[1]
    end_dt = sys.argv[2]
    start_dt = datetime.datetime.strptime(start_dt, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end_dt, "%Y%m%d")

    coins = ["BTCUSDT", "ETHUSDT"]
    binance_client = BinanceTools.create_binance_client()
    ddb_session = BinanceTools.create_ddb_client()
    db_path = "dfs://crypto_kline"
    table_name = "kline_1min"
    data = BinanceTools.get_data(
        binance_client, symbols=coins, start_dt=start_dt, end_dt=end_dt
    )
    print(data)
    BinanceTools.insert_data(ddb_session, data, db_path, table_name)
