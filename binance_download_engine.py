from binance_tools import BinanceTools
import datetime
import schedule
import time


def update_database_handler(symbols):
    """
    Update the database with the latest data for the given symbols.

    Parameters:
    symbols (list): A list of symbols to update the database for.

    Returns:
    None
    """
    global start_dt
    ddb_client = BinanceTools.create_ddb_client()
    binance_client = BinanceTools.create_binance_client()
    data = BinanceTools.get_data(
        binance_client,
        symbols,
        start_dt,
        datetime.datetime.now() + datetime.timedelta(minutes=1),
    )
    if len(data) >= len(symbols):
        start_dt = data.groupby("symbol")["open_time"].max().min().to_pydatetime()
        start_dt = start_dt + datetime.timedelta(minutes=1)

    if len(data) != 0:
        BinanceTools.insert_data(
            ddb_client, data, db_path="dfs://crypto_kline", table_name="kline_1min"
        )

    print(start_dt)
    print(data)


def update_database(symbols):
    """
    This function is responsible for scheduling tasks to run at the start of every minute.

    It schedules two tasks:
    1. `update_1min_data` - This task updates the 1-minute data.
    2. `repair_database_previous_hour` - This task repairs the database for the previous hour.

    The function runs an infinite loop to continuously check for pending tasks and sleeps for 0.5 seconds between checks.
    """
    schedule.every().minute.at(":01").do(update_database_handler, symbols)
    # schedule.every(3).seconds.do(update_database_handler, symbols)

    # this might cause connection error
    # schedule.every().minute.at(":10").do(BinanceTools.check_today_data_integrity)

    while True:
        schedule.run_pending()
        time.sleep(0.5)


def get_latest_database_time():
    symbols = ["BTCUSDT", "ETHUSDT"]
    ddb_client = BinanceTools.create_ddb_client()
    db_path = "dfs://crypto_kline"
    table_name = "kline_1min"

    dates = (
        ddb_client.loadTable(dbPath=db_path, tableName=table_name)
        .select("max(open_time) as time")
        .groupby("symbol")
        .toDF()
    )
    start_dt = dates["time"].min().to_pydatetime()
    return start_dt


if __name__ == "__main__":
    # update 1min kline data of BTCUSDT and ETHUSDT
    # BinanceTools.run_on_minute_start()

    # start_dt = datetime.datetime.now()
    start_dt = get_latest_database_time()

    symbols = ["BTCUSDT", "ETHUSDT"]

    update_database(symbols)
