from queue import Queue
from threading import Thread
import datetime
import requests
import os


def downloadonefile(file_url):

    save_path=r'F:\gxz\wrf-data\gfs'
    response = requests.get(file_url)

    # 检查响应状态码，200 表示请求成功
    if response.status_code == 200:
        # 提取文件名
        file_name = file_url.split('/')[-1]
        date_fold=os.path.join(save_path,file_url.split('/')[-4].split('.')[1])
        if not os.path.exists(date_fold):
            os.makedirs(date_fold)

        # 保存文件到本地
        with open(os.path.join(date_fold,file_name), 'wb') as file:
            file.write(response.content)
        print(f'文件已下载到 {file_name}')
    else:
        print('下载失败，状态码:', response.status_code)



# file_url = 'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20231010/00/atmos/gfs.t00z.pgrb2.0p25.f000'

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
    begin_date = datetime.date(2023, 10, 10)
    end_date = datetime.date(2023, 10, 10)
    temp_date = begin_date
    delta = datetime.timedelta(days=1)

    # 建立下载日期序列
    queue = Queue()
    while temp_date <= end_date:

        for pre_hour in range(24):

            download_url='https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{0}{1}{2}/00/atmos/gfs.t00z.pgrb2.0p25.f{3}'.\
            format(str(temp_date.year),str(temp_date.month).zfill(2),str(temp_date.day).zfill(2),str(pre_hour).zfill(3))
            queue.put(download_url)
        temp_date += delta

    # 注意，每个用户同时最多接受4个request https://cds.climate.copernicus.eu/vision
    # 创建4个工作线程
    for x in range(5):
        worker = DownloadWorker(queue)
        # 将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()

    queue.join()


if __name__ == '__main__':
    main()
