import os
import math
import glob
from osgeo import gdal

# xyz到经纬度的转换函数
def xyz2lonlat(x, y, z):
    n = 2 ** z
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_deg = math.degrees(lat_rad)
    return lon_deg, lat_deg

# 为PNG瓦片创建世界文件
def create_world_file(tile_path, x, y, z):
    lon1, lat1 = xyz2lonlat(x, y, z)
    lon2, lat2 = xyz2lonlat(x + 1, y + 1, z)

    pixelSizeX = (lon2 - lon1) / 256
    pixelSizeY = -(lat1 - lat2) / 256  # 注意这里取负值

    world_file_content = f"{pixelSizeX}\n0.0\n0.0\n{pixelSizeY}\n{lon1}\n{lat1}"

    world_file_path = os.path.splitext(tile_path)[0] + '.pgw'
    with open(world_file_path, 'w') as wf:
        wf.write(world_file_content)

# 创建GeoTIFF的主要函数
def create_geotiff(input_folder, output_tiff_path, zoom_level):
    # 获取所有PNG瓦片并为每个瓦片创建世界文件
    tiles = glob.glob(os.path.join(input_folder, '*.png'))
    for tile in tiles:
        x, y = map(int, os.path.splitext(os.path.basename(tile))[0].split('_'))
        create_world_file(tile, x, y, zoom_level)

    # 创建VRT
    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', addAlpha=True)
    vrt_dataset = gdal.BuildVRT(os.path.join(input_folder, 'temp.vrt'), tiles, options=vrt_options)

    # 输出为GeoTIFF
    gdal.Translate(output_tiff_path, vrt_dataset)
    vrt_dataset = None  # 释放资源

# 使用示例
input_folder = r'D:\code\google\layers\raster\png'  # PNG瓦片的文件夹路径
output_tiff_path = r'D:\code\google\layers\raster\tif\file.tif'  # 输出GeoTIFF文件的路径
zoom_level = 20  # 使用的缩放级别

create_geotiff(input_folder, output_tiff_path, zoom_level)
