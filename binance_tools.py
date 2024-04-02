# Periodically download BTC and ETH 1MIN Klines data using Binance API and save it to a dolphindb database

import requests
import json
import time
import dolphindb as ddb
import pandas as pd
import datetime
import schedule
import os
from binance.client import Client
from DingDingBot.DDBOT import DingDing
import time
import hmac
import hashlib
import base64
import urllib.parse
import retrying
import pytz


class BinanceTools:

    @staticmethod
    def create_bot():
        """
        Creates a DingDing bot with the specified webhook.

        Returns:
            DingDing: An instance of the DingDing class representing the bot.

        Raises:
            KeyError: If the required environment variables (DINGDING_ACCESS_TOKEN and DINGDING_SECRET) are not set.
        """
        # Initialize DingDing bot with webhook
        timestamp = str(round(time.time() * 1000))
        try:
            access_token = os.environ["DINGDING_ACCESS_TOKEN"]
            secret = os.environ["DINGDING_SECRET"]
            webhook_prefix = (
                f"https://oapi.dingtalk.com/robot/send?access_token={access_token}"
            )
            secret_enc = secret.encode("utf-8")
            string_to_sign = "{}\n{}".format(timestamp, secret)
            string_to_sign_enc = string_to_sign.encode("utf-8")
            hmac_code = hmac.new(
                secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

            webhook = f"{webhook_prefix}&timestamp={timestamp}&sign={sign}"
            return DingDing(webhook=webhook)
        except KeyError:
            raise KeyError(
                "DINGDING_ACCESS_TOKEN and DINGDING_SECRET environment variables must be set."
            )

    @staticmethod
    @retrying.retry(wait_fixed=2000, stop_max_attempt_number=10)
    def create_binance_client():
        """
        Creates a Binance client object using the provided API key and secret.

        Returns:
            Binance client object: An instance of the Binance client.

        Raises:
            KeyError: If the BINANCE_API_KEY or BINANCE_API_SECRET environment variables are not set.
        """
        try:
            api_key = os.environ["BINANCE_API_KEY"]
            api_secret = os.environ["BINANCE_API_SECRET"]
            proxies = {"https": "127.0.0.1:7890"}
            client = Client(api_key, api_secret, {"proxies": proxies})
            return client
        except KeyError:
            raise KeyError(
                "BINANCE_API_KEY and BINANCE_API_SECRET environment variables must be set."
            )

    @staticmethod
    def create_ddb_client():
        """
        Creates and returns a DDB client.

        Returns:
            DDB client: A client object for interacting with DDB.
        """
        s = ddb.session()
        s.connect("localhost", 8902, "admin", "123456")
        return s

    @staticmethod
    # create database and table
    def create_db_database_and_table():
        """
        Creates a database and table in the DDB (DolphinDB) database.

        This function connects to the DDB server, creates a database with two sub-databases,
        and creates a partitioned table within the database.

        Returns:
            None
        """
        s = ddb.session()
        s.connect("localhost", 8902, "admin", "123456")
        s.run("dbPath = 'dfs://crypto_kline'")
        s.run("tableName = 'kline_1min'")
        s.run("if(existsDatabase(dbPath)){dropDatabase(dbPath)}")
        s.run('db1 = database("", VALUE, 2020.01.01..2024.12.31)')
        s.run('db2 = database("", HASH,[SYMBOL,3])')
        s.run("db = database(dbPath,COMPO, [db1,db2], engine = 'TSDB')")
        s.run(
            "t = table(1:0, `open_time`open`high`low`close`volume`close_time`quote_asset_volume`number_of_trades`taker_buy_base_asset_volume`taker_buy_quote_asset_volume`symbol, [TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, TIMESTAMP, DOUBLE, INT, DOUBLE, DOUBLE, SYMBOL])"
        )
        s.run(
            f"db.createPartitionedTable(t, tableName, `open_time`symbol, sortColumns = `symbol`open_time, keepDuplicates = LAST)"
        )

    @staticmethod
    def insert_data(ddb_session, data, db_path, table_name):
        """
        Inserts data into DolphinDB database table.

        Args:
            ddb_session (DolphinDBSession): The DolphinDB session object.
            data (pd.DataFrame): The data to be inserted into the table.
            db_path (str): The path of the database.
            table_name (str): The name of the table.

        Raises:
            Exception: If there is an error inserting data into DolphinDB.

        """
        try:
            ddb_session.run(f"t = loadTable('{db_path}', '{table_name}')")
            ddb_session.run("append!{t}", data)
        except Exception as e:
            bot = BinanceTools.create_bot()
            bot.Send_Text_Msg(f"Error inserting data into DolphinDB: {e}")
            raise

    @staticmethod
    def convert_data(symbol, data):
        """
        Convert raw data into a pandas DataFrame with the correct data types.

        Args:
            symbol (str): The symbol associated with the data.
            data (list): The raw data to be converted.

        Returns:
            pandas.DataFrame: The converted data with the correct data types.
        """
        data = pd.DataFrame(
            data,
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
                "symbol",
            ],
        )
        data["symbol"] = symbol
        # convert data columns to the correct data type
        for col in data.columns:
            if col in ["open_time", "close_time"]:
                data[col] = data[col].apply(
                    lambda x: datetime.datetime.fromtimestamp(
                        x / 1000
                    )  # the timestamp is in local timezone
                )
            elif col != "symbol":
                data[col] = pd.to_numeric(data[col])
        return data

    @staticmethod
    def get_latest_data(client, symbols):
        """
        Retrieves the latest data for the given symbols from the Binance API.

        Args:
            client (binance.Client): The Binance API client.
            symbols (list): A list of symbols to retrieve data for.

        Returns:
            pandas.DataFrame: The latest data for the given symbols.
        """
        try:
            data = pd.DataFrame()
            for symbol in symbols:
                cur_data = client.get_klines(
                    symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1
                )
                cur_data = BinanceTools.convert_data(symbol, cur_data)
                data = pd.concat([data, cur_data])
        except Exception as e:
            bot = BinanceTools.create_bot()
            bot.Send_Text_Msg(f"Error getting latest data from Binance API: {e}")
        return data

    @staticmethod
    def get_data(client, symbols, start_dt, end_dt):
        """
        Retrieves historical data for the specified symbols from Binance.

        Args:
            client (binance.Client): The Binance API client.
            symbols (list): A list of symbols to retrieve data for.
            start_dt (datetime.datetime): The start date and time.
            end_dt (datetime.datetime): The end date and time.

        Returns:
            pandas.DataFrame: The historical data for the specified symbols. Data at the end_dt is excluded.
        """
        start_dt = BinanceTools.to_utc(start_dt)
        end_dt = BinanceTools.to_utc(end_dt)

        start_dt = int(start_dt.timestamp() * 1000)
        end_dt = int(end_dt.timestamp() * 1000) - 1

        data = pd.DataFrame()
        for symbol in symbols:
            cur_data = client.get_historical_klines(
                symbol, Client.KLINE_INTERVAL_1MINUTE, start_dt, end_dt
            )
            cur_data = BinanceTools.convert_data(symbol, cur_data)
            data = pd.concat([data, cur_data])
        return data

    @staticmethod
    def check_values(db_path, table_name):
        """
        Check values in the specified database table. each symbol should have 1440 records per day corresponding to 1-minute data.

        Args:
            db_path (str): The path to the database.
            table_name (str): The name of the table to check.

        Returns:
            None
        """
        ddb_session = BinanceTools.create_ddb_client()
        t = ddb_session.loadTable(dbPath=db_path, tableName=table_name)
        values = (
            t.select("count(*) as c, symbol, d")
            .groupby(["symbol", "date(open_time) as d"])
            .toDF()
        )
        values = values.loc[values["c"] != 1440]
        today = datetime.datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        values = values.loc[values["d"] != today]
        if len(values) != 0:
            client = BinanceTools.create_binance_client()
            for _, row in values.iterrows():
                start_dt = row["d"]
                end_dt = start_dt + datetime.timedelta(days=1)
                symbol = row["symbol"]
                data = BinanceTools.get_data(client, [symbol], start_dt, end_dt)
                print(symbol, start_dt, end_dt, row["c"], len(data))
                BinanceTools.insert_data(ddb_session, data, db_path, table_name)

    @staticmethod
    def get_date_range(start_date, end_date):
        """
        Generate a list of dates within a given date range.

        Args:
            start_date (datetime.date): The start date of the range.
            end_date (datetime.date): The end date of the range.

        Returns:
            list: A list of dates within the specified range.

        """
        date_list = []
        current_date = start_date

        while current_date <= end_date:
            date_list.append(current_date)
            current_date += datetime.timedelta(days=1)

        return date_list

    @staticmethod
    def to_utc(date):
        """
        Converts a given date to UTC timezone.

        Args:
            date (datetime): The date to be converted.

        Returns:
            datetime: The converted date in UTC timezone.
        """
        local_tz = pytz.timezone("Asia/Shanghai")
        date = local_tz.localize(date)
        return date.astimezone(pytz.utc)

    @staticmethod
    def update_1min_data():
        """
        Updates 1-minute data by downloading data from Binance API and inserting it into a DynamoDB table.

        This function runs in an infinite loop, downloading data every minute and inserting it into the specified DynamoDB table.
        It ensures that the script runs every minute at the first second of the minute.

        Raises:
            Exception: If there is an error in the main function, it sends an error message via a bot.

        """
        try:
            binance_client = BinanceTools.create_binance_client()
            ddb_client = BinanceTools.create_ddb_client()
            coins = ["BTCUSDT", "ETHUSDT"]
            db_path = "dfs://crypto_kline"
            table_name = "kline_1min"
            get_correct_data = False
            max_try = 10
            cur_time = datetime.datetime.now()
            while not get_correct_data and max_try > 0:
                data = BinanceTools.get_latest_data(binance_client, coins)
                print(
                    "Updating data... at ",
                    cur_time.strftime("%Y-%m-%d %H:%M:%S"),
                )
                get_correct_data = (
                    data["open_time"].dt.minute == cur_time.minute
                ).all()
                if get_correct_data:
                    print(data)
                    BinanceTools.insert_data(ddb_client, data, db_path, table_name)
                else:
                    time.sleep(1)
                    max_try -= 1
        except Exception as e:
            bot = BinanceTools.create_bot()
            bot.Send_Text_Msg(f"Error in main function: {e}")

    @staticmethod
    def repair_database_previous_hour():
        """
        Repairs the database by retrieving missing data for the previous hour.

        This function calculates the start and end time for the previous hour,
        queries the database to check the number of data points for each symbol,
        and retrieves missing data from the Binance API if necessary.

        Args:
            None

        Returns:
            None
        """
        print("repair database....")
        db_path = "dfs://crypto_kline"
        table_name = "kline_1min"
        # Calculate the start and end time for the previous hour
        end_time = datetime.datetime.now().replace(microsecond=0, second=0)
        start_time = (end_time - datetime.timedelta(hours=1)).strftime(
            "%Y.%m.%d %H:%M:%S"
        )
        end_time = end_time.strftime("%Y.%m.%d %H:%M:%S")

        # Query the database to check the number of data points for each symbol
        count_query = f"select count(*) from loadTable({db_path}, {table_name}) where open_time >= {start_time} and open_time < {end_time}"
        ddb_session = BinanceTools.create_ddb_client()
        symbols = ["BTCUSDT", "ETHUSDT"]
        for symbol in symbols:
            result = (
                ddb_session.loadTable(dbPath=db_path, tableName=table_name)
                .select("count(*)")
                .where(f"open_time >= {start_time}")
                .where(f"open_time < {end_time}")
                .where(f"symbol=`{symbol}")
            ).toList()
            if result[0][0] != 60:
                print(symbol, result[0][0])
                client = BinanceTools.create_binance_client()
                start_dt = int(
                    datetime.datetime.strptime(
                        start_time, "%Y.%m.%d %H:%M:%S"
                    ).timestamp()
                    * 1000
                )
                end_dt = int(
                    datetime.datetime.strptime(
                        end_time, "%Y.%m.%d %H:%M:%S"
                    ).timestamp()
                    * 1000
                )
                data = client.get_historical_klines(
                    symbol, Client.KLINE_INTERVAL_1MINUTE, start_dt, end_dt
                )
                data = BinanceTools.convert_data(symbol, data)
                BinanceTools.insert_data(ddb_session, data, db_path, table_name)

    @staticmethod
    def run_on_minute_start():
        """
        This function is responsible for scheduling tasks to run at the start of every minute.

        It schedules two tasks:
        1. `update_1min_data` - This task updates the 1-minute data.
        2. `repair_database_previous_hour` - This task repairs the database for the previous hour.

        The function runs an infinite loop to continuously check for pending tasks and sleeps for 0.5 seconds between checks.
        """
        schedule.every().minute.at(":01").do(BinanceTools.update_1min_data)
        schedule.every().minute.at(":10").do(BinanceTools.check_today_data_integrity)

        while True:
            schedule.run_pending()
            time.sleep(0.5)

    @staticmethod
    def check_today_data_integrity():
        """
        Checks the integrity of today's data in the Binance database.

        Returns:
            None
        """
        ddb_session = BinanceTools.create_ddb_client()
        today = datetime.datetime.today()
        cur_hour = today.hour
        cur_minute = today.minute
        today_str = today.strftime("%Y.%m.%d")
        counts = (
            ddb_session.loadTable(dbPath="dfs://crypto_kline", tableName="kline_1min")
            .select("symbol, count(*) as size")
            .where(f"date(open_time) = {today_str}")
            .groupby("symbol")
            .toDF()
        )
        correct_count = cur_hour * 60 + cur_minute + 1
        for _, row in counts.iterrows():
            print(
                row["symbol"], row["size"] == correct_count, row["size"], correct_count
            )

        if (counts["size"] != correct_count).any():
            BinanceTools.repair_database_previous_hour()


if __name__ == "__main__":
    # BinanceTools.check_today_data_integrity()
    BinanceTools.run_on_minute_start()
