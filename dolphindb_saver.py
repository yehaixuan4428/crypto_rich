# -*- coding: UTF-8 -*-
'''
@Project    : crypto 
@File       : dolphindb_saver.py
@Author     : Haixuan Ye (haixuanye@outlook.com)
@Date       : 2024/3/2 20:52 
@Description: 
'''

from dolphindb import session as ddb_session
import pandas as pd


class DolphinDBKlineSaver:
    def __init__(self,
                 host='localhost',
                 port=7777,
                 db_path='/path/to/db',
                 table_name='kline_data'):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.table_name = table_name
        self.session = ddb_session()
        self.connected = self.session.connect(self.host,
                                              self.port)

        if not self.connected:
            raise ConnectionError("Failed to connect to DolphinDB server.")

        # 初始化数据库和表
        self.init_db_and_table()

    def init_db_and_table(self):
        # 创建数据库和表的DolphinDB脚本
        script = f"""
        db = database("{self.db_path}")

        if(!existsTable('{self.db_path}', "{self.table_name}")) {{
            schema = table(100:0, `symbol`open_time`close_time`open`high`low`close`volume`quote_asset_volume`number_of_trades`taker_buy_base_asset_volume`taker_buy_quote_asset_volume, [SYMBOL,TIMESTAMP,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,INT,DOUBLE,DOUBLE])
            db.createPartitionedTable(schema, "{self.table_name}", `open_time).append!
        }}
        """
        self.session.run(script)

    def save_klines_to_dolphindb(self,
                                 klines_df):
        if not self.connected:
            raise ConnectionError("Not connected to DolphinDB server.")

        # 确保DataFrame的index包含symbol和open_time
        if not isinstance(klines_df.index,
                          pd.MultiIndex) or 'symbol' not in klines_df.index.names or 'open_time' not in klines_df.index.names:
            raise ValueError("DataFrame index must be a MultiIndex with 'symbol' and 'open_time' levels.")

        # 重置索引，准备数据上传
        df_reset = klines_df.reset_index()

        # 上传DataFrame到DolphinDB服务器
        self.session.upload({
            self.table_name: df_reset})

        # 将数据插入到DolphinDB表中
        self.session.run(f"t = loadTable('{self.db_path}', '{self.table_name}').append!({self.table_name})")
