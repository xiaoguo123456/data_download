from gevent import monkey
# 在进行IO操作时，默认切换协程
monkey.patch_all()
import gevent
from gevent.queue import Queue
import requests
import re
import os
from bs4 import BeautifulSoup
from tqdm import tqdm

os.environ["http_proxy"] = "http://127.0.0.1:10810"
os.environ["https_proxy"] = "http://127.0.0.1:10810"


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
def url_generater(save_path,in_queue,out_queue):
    while not in_queue.empty():
        url=in_queue.get_nowait()
        time_path = os.path.join(save_path, url.split('/')[-2])
        if not os.path.exists(time_path):
            os.makedirs(time_path)
        time_content = BeautifulSoup(session.get(url).content,features="lxml").find_all('a')
        for k in time_content:
            tile_name = re.findall(
                r'MOD....\.A[0-9]*\.(?:h25v03|h26v03|h23v04|h24v04|h25v04|h26v04|h27v04|h23v05|h24v05|h25v05|h26v05|h26v05|h28v05|h25v06|h26v06|h27v06|h28v06|h29v06|h28v07)\.061\.[0-9]*\.hdf$',
                k['href'])
            if len(tile_name) > 0:
                tile_url = os.path.join(url, tile_name[0])
                tile_path = os.path.join(time_path, tile_name[0])
                out_queue.put_nowait(
                    {'tile_url': tile_url,
                     'tile_path': tile_path
                     })



def download(session, queue):
    while not queue.empty():
        date_dict=queue.get_nowait()
        tile_url, tile_path = date_dict.values()
        if (os.path.isfile(tile_path)):
            print("ok", tile_path)
        else:
            response = session.get(tile_url).content
            with open(tile_path, mode='wb') as f:
                f.write(response)
            print(tile_url.split('/')[-1], '：下载成功')


if __name__ == '__main__':
    china_tiles = ['h25v03', 'h26v03', 'h23v04', 'h24v04', 'h25v04', 'h26v04', 'h27v04', 'h23v05', 'h24v05',
                   'h25v05', 'h26v05', 'h26v05', 'h28v05', 'h25v06', 'h26v06', 'h27v06', 'h28v06', 'h29v06', 'h28v07']

    url = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD09A1.061/'

    username = 'guoxiaozheng123'
    password = 'ybqLDijfFCbP4m4'

    session = SessionWithHeaderRedirection(username, password)
    response = session.get(url)

    root_content = BeautifulSoup(response.content,features="lxml").find_all('a')

    in_queue = Queue()
    for i in root_content:
        temp_url = re.findall(r'....\...\.../', i['href'])
        if len(temp_url) > 0:
            in_queue.put_nowait(os.path.join(url, temp_url[0]))

    save_path = r'F:\raster\MODIS\MOD09A1'
    out_queue = Queue()

    a_threads = []
    for x in range(20):
        task = gevent.spawn(url_generater, save_path,in_queue,out_queue)
        a_threads.append(task)
    gevent.joinall(a_threads)

    # download(session, out_queue)
    b_threads = []
    for x in range(5):
        task = gevent.spawn(download, session,out_queue)
        b_threads.append(task)
    gevent.joinall(b_threads)



