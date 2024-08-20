import cdsapi
import os
from datetime import datetime, timedelta
import threading
from queue import Queue

class Era5Downloader:
    def __init__(self, dataset, variable, start_date, end_date, save_path, max_threads=4):
        self.client = cdsapi.Client()
        self.dataset = dataset
        self.variable = variable
        self.start_date = start_date
        self.end_date = end_date
        self.save_path = save_path
        self.max_threads = max_threads
        self.queue = Queue()
        self._prepare_queue()

        if not os.path.exists(save_path):
            os.makedirs(save_path)

    def _prepare_queue(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            for hour in range(24):  # 一天内的所有时间点
                time = f"{hour:02d}:00"
                # 更新文件名格式，只保留年月日和小时
                save_file = os.path.join(
                    self.save_path,
                    f"{self.variable[0]}_{current_date.strftime('%Y%m%d')}_{hour:02d}.nc"
                )
                self.queue.put((current_date, time, save_file))
            current_date += timedelta(days=1)

    def _download_data(self):
        while not self.queue.empty():
            date, time, save_file = self.queue.get()
            if os.path.exists(save_file):
                print(f"File {save_file} already exists, skipping download.")
                continue

            request = {
                'variable': self.variable,
                'year': date.strftime('%Y'),
                'month': date.strftime('%m'),
                'day': date.strftime('%d'),
                'time': [time],
                'format': 'netcdf'
            }

            print(f"Downloading data for {date.strftime('%Y-%m-%d')} {time} to {save_file}")
            try:
                self.client.retrieve(self.dataset, request).download(save_file)
            except Exception as e:
                print(f"Failed to download data for {date.strftime('%Y-%m-%d')} {time}: {e}")
            finally:
                self.queue.task_done()

    def start_download(self):
        threads = []
        for _ in range(self.max_threads):
            thread = threading.Thread(target=self._download_data)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    # 配置部分
    dataset = "reanalysis-era5-land"
    variable = ['2m_temperature']  # 可以修改为其他变量
    start_date = datetime(2017, 11, 1)  # 设置起始时间
    end_date = datetime(2017, 11, 30)  # 设置结束时间
    save_path = r'D:\每日工作\era5下载\data'  # 设置保存文件夹路径
    max_threads = 4  # 设置最大线程数

    # 初始化并启动下载
    downloader = Era5Downloader(dataset, variable, start_date, end_date, save_path, max_threads)
    downloader.start_download()
