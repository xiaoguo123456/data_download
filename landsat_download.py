from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer


# Initialize a new API instance and get an access key
api = API('landsat8guoxiaozheng', 'N34EWUxTy2jizXR')

# Search for Landsat scenes
# 搜索Landsat影像
scenes = api.search(
    dataset='landsat_ot_c2_l1',  # 数据集名称
    latitude=34.0522,
    longitude=-118.2437,
    start_date='2020-01-01',
    end_date='2020-12-31',
    max_cloud_cover=10
)

print(f"{len(scenes)} scenes found.")

# 下载影像
ee = EarthExplorer('landsat8guoxiaozheng', 'N34EWUxTy2jizXR')
for scene in scenes:

    ee.download(identifier=scene['entity_id'], output_dir=r'D:\code\landsat_download\data')

# 关闭连接
ee.logout()
api.logout()
