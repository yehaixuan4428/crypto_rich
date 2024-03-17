# -*- coding: UTF-8 -*-
'''
@Project    : crypto 
@File       : incrementalDataUpdater.py
@Author     : Haixuan Ye (haixuanye@outlook.com)
@Date       : 2024/3/2 20:54 
@Description: 
'''

class IncrementalDataUpdater:
    def __init__(self,
                 downloader,
                 saver,
                 symbols):
        """
        :param downloader: ParallelDownloader 实例，用于下载数据。
        :param saver: DolphinDBKlineSaver 实例，用于保存数据到 DolphinDB。
        :param symbols: 要更新的交易对列表。
        """
        self.downloader = downloader
        self.saver = saver
        self.symbols = symbols

    def get_latest_open_time(self,
                             symbol):
        """
        查询给定交易对在 DolphinDB 中的最新 open_time。
        :param symbol: 交易对。
        :return: 最新的 open_time。
        """
        query = f"select max(open_time) as latest_open_time from loadTable('{self.saver.db_path}', '{self.saver.table_name}') where symbol='{symbol}'"
        result = self.saver.session.run(query)
        if result:
            return result[0][0]  # 返回查询结果中的最新 open_time
        else:
            return None

    def update_data(self,
                    end_date):
        """
        下载并更新从最新 open_time 到指定日期的增量数据。
        :param end_date: 结束日期。
        """
        for symbol in self.symbols:
            latest_open_time = self.get_latest_open_time(symbol)
            if latest_open_time:
                # 如果存在最新 open_time，则下载从最新 open_time 到 end_date 的增量数据
                start_date = (latest_open_time + pd.Timedelta(minutes=1)).strftime('%Y-%m-%d')  # 从最新 open_time 的下一秒开始
                self.downloader.add_task(symbol,
                                         start_date,
                                         end_date)

                # 获取下载的结果并保存到 DolphinDB
                while not self.downloader.result_queue.empty():
                    result_df = self.downloader.get_result()
                    if not result_df.empty:
                        self.saver.save_klines_to_dolphindb(result_df)
            else:
                # 如果没有最新的 open_time（即数据库中没有该交易对的数据），可以选择下载完整的数据或跳过
                pass


