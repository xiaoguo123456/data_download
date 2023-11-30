import math
import os
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def swap(a, b):
    a, b = b, a
    return a, b


class Point:
    def __init__(self, lon, lat):
        self.lon = lon
        self.lat = lat


class Box:
    def __init__(self, point_lt, point_rb):
        self.point_lt = point_lt
        self.point_rb = point_rb


def build_url(x, y, z):
    return "http://mts0.googleapis.com/vt?lyrs=s&x={x}&y={y}&z={z}".format(x=x, y=y, z=z)


def download(url, filepath):
    if not os.path.isfile(filepath):
        print('[Download]:',filepath)

        response = requests.get(url)

        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
        else:
            print("network error!")
    else:
        return None




def xyz2lonlat(x, y, z):
    n = math.pow(2, z)
    lon = x / n * 360.0 - 180.0
    lat = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = lat * 180.0 / math.pi
    return lon, lat


def lonlat2xyz(lon, lat, zoom):
    n = math.pow(2, zoom)
    x = ((lon + 180) / 360) * n
    y = (1 - (math.log(math.tan(math.radians(lat)) + (1 / math.cos(math.radians(lat)))) / math.pi)) / 2 * n
    return int(x), int(y)


def cal_tiff_box(x1, y1, x2, y2, z):
    LT = xyz2lonlat(x1, y1, z)
    RB = xyz2lonlat(x2 + 1, y2 + 1, z)
    return Point(LT[0], LT[1]), Point(RB[0], RB[1])

def get_all_urls(x1, y1, x2, y2, z):

    urlArray = [] 
    for i in range(x1, x2+1):
        for j in range(y1, y2+1):
            xy_name= "{}_{}.png".format(i,j)
            urlArray.append([build_url(i,j,z),xy_name])

    return urlArray


def downloadPlus(url_list, save_path,thread_num):

    
    with ThreadPoolExecutor(max_workers=thread_num) as executor:
        for url,file_name in url_list:
            file_path = os.path.join(save_path, file_name)

            executor.submit(download,url,file_path)

def core(point_lt,point_rb,path,z,thread_num):
    x1, y1 = lonlat2xyz(point_lt.lon, point_lt.lat, z)
    x2, y2 = lonlat2xyz(point_rb.lon, point_rb.lat, z)
    print(x1, y1, z)
    print(x2, y2, z)
    print((x2-x1+1) * (y2-y1+1))
    url_name=get_all_urls(x1, y1, x2, y2, z)
    downloadPlus(url_name,path,thread_num)

def check_and_download_tiles(x1, y1, x2, y2, z, save_path):
    missing_tiles = []

    for x in range(x1, x2 + 1):
        for y in range(y1, y2 + 1):
            tile_path = os.path.join(save_path, f"{x}_{y}.png")
            if not os.path.exists(tile_path):
                missing_tiles.append((x, y))

    if missing_tiles:
        print(f"Found {len(missing_tiles)} missing tiles. Starting download...")
        with ThreadPoolExecutor(max_workers=15) as executor:
            for x, y in missing_tiles:
                url = build_url(x, y, z)
                file_path = os.path.join(save_path, f"{x}_{y}.png")
                executor.submit(download, url, file_path)
    else:
        print("All tiles are present.")

# 检查瓦片的函数
def check_tiles(point_lt, point_rb, path, z):
    x1, y1 = lonlat2xyz(point_lt.lon, point_lt.lat, z)
    x2, y2 = lonlat2xyz(point_rb.lon, point_rb.lat, z)
    check_and_download_tiles(x1, y1, x2, y2, z, path)
    
if __name__ == '__main__':
    # 存储目录
    path = r"D:\code\google\layers\raster\neihuang\png"
    # 下载范围的 左上点经纬度
    point_lt = Point(114.578887530435, 36.146283795161)
    # 下载范围的 右下点经纬度
    point_rb = Point(114.982496025171,35.6426366450651)
    # 开始级别 
    zoom = 20



    core(point_lt,point_rb,path,20,15)

    check_tiles(point_lt,point_rb,path,20)

