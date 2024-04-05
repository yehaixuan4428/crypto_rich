from binance_tools import BinanceTools
import datetime

if __name__ == "__main__":
    # update 1min kline data of BTCUSDT and ETHUSDT
    BinanceTools.run_on_minute_start()
