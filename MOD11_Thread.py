import requests
import re
import os
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread
import time
from tqdm import tqdm

os.environ["http_proxy"] = "http://127.0.0.1:10809"
os.environ["https_proxy"] = "http://127.0.0.1:10809"

def downloadonefile(parameter):

    url,file_name=parameter.values()
    file_path=os.path.join(r"H:\shop\20221205\200\data",file_name)

    if (os.path.isfile(file_path)):  # 如果存在文件名则返回
        print("ok", file_name)
    else:
        response = requests.get(url).content
        with open(file_path, mode='wb') as f:
            f.write(response)
        print(url.split('/')[-1], '：下载成功')

class DownloadWorker(Thread):
    def __init__(self, session,queue):
        Thread.__init__(self)
        self.queue = queue
        self.session = session

    def run(self):
        while True:
            # 从队列中获取任务并扩展tuple
            riqi = self.queue.get()
            downloadonefile(self.session,riqi)
            self.queue.task_done()

class SessionWithHeaderRedirection(requests.Session):

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):

        super().__init__()

        self.auth = (username, password)

   # the NASA auth host.

    def rebuild_auth(self, prepared_request, response):

        headers = prepared_request.headers

        url = prepared_request.url

        if 'Authorization' in headers:

            original_parsed = requests.utils.urlparse(response.request.url)

            redirect_parsed = requests.utils.urlparse(url)



            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:

                del headers['Authorization']
        return

def downloadonefile(session,parameter):

    url,file_path=parameter.values()


    if (os.path.isfile(file_path)):  # 如果存在文件名则返回
        print("ok", file_path)
    else:
        response = session.get(url).content
        with open(file_path, mode='wb') as f:
            f.write(response)
        print(url.split('/')[-1], '：下载成功')



if __name__ == '__main__':


    url = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.061/'
    username = 'earthdataguo'
    password = 'Fendou1996127'

    session = SessionWithHeaderRedirection(username, password)
    response = session.get(url)
    root_content = BeautifulSoup(response.content).find_all('a')
    time_url = []
    for i in root_content:
        temp_url = re.findall(r'(?:2015|2016|2017|2018|2019|2020|2021)\...\.../', i['href'])
        if len(temp_url) > 0:
            time_url.append(os.path.join(url, temp_url[0]))

    save_path = r'H:\shop\20221118\multi_variable\raw_data\MOD11A1'

    queue = Queue()

    for j in tqdm(time_url):
        t1 = time.time()
        time_path = os.path.join(save_path, j.split('/')[-2])
        print(j.split('/')[-1])
        if not os.path.exists(time_path):
            os.makedirs(time_path)
        time_content = BeautifulSoup(session.get(j).content).find_all('a')
        for k in time_content:
            tile_name = re.findall(
                r'MOD11A1\.A[0-9]*\..*\.061\.[0-9]*\.hdf$',
                k['href'])

            if len(tile_name) > 0:
                tile_url = os.path.join(j, tile_name[0])
                tile_path = os.path.join(time_path, tile_name[0])
                # pool.apply_async(func=download, args=(session, tile_url, tile_path,))
                if os.path.exists(tile_path):
                    continue
                queue.put(
                    {'tile_url':tile_url,
                     'tile_path':tile_path
                     }
                )

    for x in range(20):
        worker = DownloadWorker(session,queue)
        # 将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()

    queue.join()





