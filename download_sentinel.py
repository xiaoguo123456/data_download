import os
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt


os.environ["http_proxy"] = "http://127.0.0.1:10810"
os.environ["https_proxy"] = "http://127.0.0.1:10810"

api = SentinelAPI('boyguo', 'Fendou1996127.')
#geojson.io
footprint = geojson_to_wkt(read_geojson(r'F:\code\sentinelsat\map.geojson'))
products = api.query(footprint,
                    date=('20220510','20221012'),
                    platformname='Sentinel-2',
                    producttype='S2MSI1C',
                    cloudcoverpercentage=(0,20),
                     limit=10)
for product in products:
	#通过OData API获取单一产品数据的主要元数据信息
	product_info = api.get_product_odata(product)
	#打印下载的产品数据文件名
	print(product_info['title'])
	#下载产品id为product的产品数据
	api.download(product,directory_path=r"F:\code\sentinelsat")
