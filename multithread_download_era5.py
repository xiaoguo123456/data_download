from queue import Queue
from threading import Thread
import cdsapi
import datetime
import os

os.environ["http_proxy"] = "http://127.0.0.1:10810"
os.environ["https_proxy"] = "http://127.0.0.1:10810"

def downloadonefile(riqi):

    year, month, day=riqi.values()
    filename = r"F:\\raster\\ERA5\\2m_temperature\\" + 'ERA5_2m_temperature_{}_{}_{}'.format(year, month, day) + ".nc"
    if (os.path.isfile(filename)):  # 如果存在文件名则返回
        print("ok", filename)
    else:
        print(filename)
        c = cdsapi.Client()
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',  # Supported format: grib and netcdf. Default: grib
                'variable': '2m_temperature',
                'year': year,
                'month': month,
                'day': day,
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00'
                ],

            },
            filename)


# 下载脚本
class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # 从队列中获取任务并扩展tuple
            riqi = self.queue.get()
            downloadonefile(riqi)
            self.queue.task_done()


# 主程序
def main():


    # 起始日期
    begin_date = datetime.date(1980, 1, 1)
    end_date = datetime.date(2021, 12, 31)
    temp_date = begin_date
    delta = datetime.timedelta(days=1)

    # 建立下载日期序列
    queue = Queue()
    while temp_date <= end_date:

        queue.put(
            {'year':str(temp_date.year),
             'month': str(temp_date.month).zfill(2),
             'day': str(temp_date.day).zfill(2)
             })
        temp_date += delta

    # 注意，每个用户同时最多接受4个request https://cds.climate.copernicus.eu/vision
    # 创建4个工作线程
    for x in range(4):
        worker = DownloadWorker(queue)
        # 将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()

    queue.join()


if __name__ == '__main__':
    main()