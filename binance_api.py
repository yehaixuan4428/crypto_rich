# -*- coding: UTF-8 -*-
"""
@Project    : crypto
@File       : binance_api.py
@Author     : Haixuan Ye (haixuanye@outlook.com)
@Date       : 2024/2/28 20:37
@Description:
"""

import threading
import queue
import time
import pandas as pd
from datetime import datetime, timedelta
from binance.spot import Spot
from binance.error import ClientError
import os
from pathlib import Path
import random
from glob import glob


class ParallelDownloader:
    def __init__(
        self,
        num_workers,
        proxies,
        saver=None,
        save_path=None,
        request_interval=1,
        limit=1000,
        max_retries=5,
    ):
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.saver = saver
        self.save_path = save_path
        self.proxies = proxies
        self.request_interval = request_interval
        self.limit = limit
        self.max_retries = max_retries
        self.workers = [
            threading.Thread(target=self.worker) for _ in range(num_workers)
        ]
        for worker in self.workers:
            worker.daemon = True
            worker.start()

    def save_to_file(self, df):
        if len(df) != 0:
            df_date = df.index.levels[1][0].date()
            df_year = df_date.year
            df_date = df_date.strftime("%Y%m%d")
            symbol = df.index.levels[0][0]
            output_folder = os.path.join(self.save_path, f"{df_year}", f"{df_date}")
            output_filename = os.path.join(output_folder, f"{symbol}_{df_date}.csv")
            if not os.path.exists(
                os.path.join(self.save_path, f"{df_year}", f"{df_date}")
            ):
                Path(output_folder).mkdir(parents=True, exist_ok=True)
            df.to_csv(output_filename)

    def worker(self):
        client = Spot(proxies=self.proxies)
        while True:
            task = self.task_queue.get()
            if task is None:
                break
            symbol, interval, start_date, end_date = (
                task["symbol"],
                task["interval"],
                task["start_date"],
                task["end_date"],
            )

            all_klines = []
            self.fetch_all_klines(
                client, symbol, interval, start_date, end_date, all_klines
            )
            df = ParallelDownloader.convert_to_dataframe(
                symbol, all_klines
            )  # 传入symbol
            print(df.head(5))
            if len(df) != 0:
                if self.saver:
                    self.saver.save_klines_to_dolphindb(df)
                if self.save_path:
                    self.save_to_file(df)
            else:
                self.result_queue.put(df)
            time.sleep(random.randint(self.request_interval, 2 * self.request_interval))

    def fetch_all_klines(
        self, client, symbol, interval, start_date, end_date, all_klines
    ):
        retries = 0
        last_end_ts = None

        end_ts = int(end_date.timestamp() * 1000) - 1
        while retries < self.max_retries:
            try:
                start_ts = (
                    int(start_date.timestamp() * 1000)
                    if last_end_ts is None
                    else last_end_ts + 1
                )
                klines = client.klines(
                    symbol=symbol,
                    interval=interval,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=self.limit,
                )

                if not klines or len(klines) < 1:
                    break

                all_klines.extend(klines)
                last_kline = klines[-1]
                last_end_ts = last_kline[0]

                if len(klines) < self.limit:
                    break

            except ClientError as e:
                if e.status_code in [429, 418]:
                    time.sleep((2**retries) * 60)
                    retries += 1
                else:
                    raise e

    @staticmethod
    def convert_to_dataframe(symbol, klines):
        df = pd.DataFrame(
            klines,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )

        df.drop(columns=["ignore"], inplace=True)

        df["open_time"] = df["open_time"].apply(
            lambda x: datetime.fromtimestamp(x / 1000)
        )
        df["close_time"] = df["close_time"].apply(
            lambda x: datetime.fromtimestamp(x / 1000)
        )

        # 设置MultiIndex：第一级是symbol，第二级是open_time
        df.set_index(["open_time"], inplace=True)
        df.index = pd.MultiIndex.from_product(
            [[symbol], df.index], names=["symbol", "open_time"]
        )
        # 转换数据类型
        for col in [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
        ]:
            df[col] = df[col].astype(float)
        return df

    def add_task(self, symbol, interval, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        self.task_queue.put(
            {
                "symbol": symbol,
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    def get_result(self):
        return self.result_queue.get()

    def stop_workers(self):
        for _ in self.workers:
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join()

    @staticmethod
    def get_date_range(start_date, end_date):
        date_list = []
        current_date = start_date

        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)

        return date_list

    def get_symbols(self, quoteAsset="USDT"):
        client = Spot(proxies=self.proxies)
        exchange_info = client.exchange_info()
        symbols = [
            symbol["symbol"]
            for symbol in exchange_info["symbols"]
            if symbol["isSpotTradingAllowed"]
            and symbol["isMarginTradingAllowed"]
            and symbol["quoteAsset"] == quoteAsset
        ]
        return symbols

    def get_latest_date(self):
        last_year = sorted(glob(os.path.join(self.save_path, "*")))[-1]
        last_date = sorted(glob(os.path.join(self.save_path, last_year, "*")))[-1]
        return datetime.strptime(last_date, "%Y%m%d")


if __name__ == "__main__":
    # # 使用示例
    proxies = {"https": "127.0.0.1:7890"}

    downloader = ParallelDownloader(
        num_workers=16,
        proxies=proxies,
        request_interval=1,
        limit=200,
        save_path="D:/crypto_data",
        max_retries=100,
    )

    # # 示例：添加下载任务
    interval = "1m"
    start_date = datetime(2020, 8, 20)
    end_date = datetime(2021, 2, 28)
    # start_date = downloader.get_latest_date() + timedelta(days=1)
    today = datetime.today()
    end_date = datetime(today.year, today.month, today.day) - timedelta(days=1)

    dates = ParallelDownloader.get_date_range(start_date, end_date)
    cryptos = downloader.get_symbols()
    for date in dates:
        for symbol in cryptos:
            if not os.path.exists(
                os.path.join(
                    "D:/crypto_data",
                    f"{date.year}",
                    date.strftime("%Y%m%d"),
                    f"{symbol}_{date.strftime('%Y%m%d')}.csv",
                )
            ):
                downloader.add_task(
                    symbol,
                    interval,
                    date.strftime("%Y-%m-%d"),
                    (date + timedelta(days=1)).strftime("%Y-%m-%d"),
                )
    result_list = []
    while not downloader.result_queue.empty():
        result_list.append(downloader.result_queue.get())

    import pickle

    with open("./wrong_cryptos.pickle", "wb") as f:
        pickle.dump(result_list, f, pickle.HIGHEST_PROTOCOL)

    # 停止所有工作线程
    downloader.stop_workers()
