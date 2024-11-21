import cdsapi
import os
from datetime import datetime, timedelta
import threading
from queue import Queue


class Era5Downloader:
    def __init__(self, dataset, variable, start_date, end_date, save_path, max_threads=4):
        self.client = cdsapi.Client(url='https://cds.climate.copernicus.eu/api', key='12521b1b-6be2-4e0b-9a5e-0c7f418bdc3a')
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
            # 更新文件名格式，只保留年月日
            save_file = os.path.join(
                self.save_path,
                f"era5_{current_date.strftime('%Y%m')}.nc"  # 存储为每月的文件
            )
            self.queue.put((current_date, save_file))
            # 增加一个月
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

    def _download_data(self):
        while not self.queue.empty():
            date, save_file = self.queue.get()
            if os.path.exists(save_file):
                print(f"File {save_file} already exists, skipping download.")
                self.queue.task_done()
                continue

            request = {

                'variable': self.variable,
                'year': date.strftime('%Y'),
                'month': date.strftime('%m'),
                'day': [str(day).zfill(2) for day in range(1, 32)],  # 请求整个月的数据
                'time': [f"{hour:02d}:00" for hour in range(24)],
                "download_format": "unarchived",
                'area': [54, 115, 38, 136] ,
                'data_format': 'netcdf'
            }

            print(f"Downloading data for {date.strftime('%Y-%m')} to {save_file}")
            try:
                self.client.retrieve(self.dataset, request).download(save_file)
            except Exception as e:
                print(f"Failed to download data for {date.strftime('%Y-%m')}: {e}")
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
    variable = [
        "total_precipitation"
    ]

    start_date = datetime(1980, 1,1)
    end_date = datetime(2020, 12,31)
    save_path = r"E:\shop\20241121\data"

    downloader = Era5Downloader(dataset, variable, start_date, end_date, save_path)
    downloader.start_download()
