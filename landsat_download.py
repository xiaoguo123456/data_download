import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer


# 配置多线程下载函数
def download_scene(scene, output_dir, username, password):
    ee = EarthExplorer(username, password)
    try:
        ee.download(identifier=scene['entity_id'], output_dir=output_dir)
        result = (scene['entity_id'], True)  # 下载成功
    except Exception as e:
        print(f"Error downloading {scene['entity_id']}: {e}")
        result = (scene['entity_id'], False)  # 下载失败
    finally:
        ee.logout()
    return result


# 主程序
def main(username, password, output_dir, max_workers=4):
    # 初始化API并搜索Landsat影像
    api = API(username, password)
    scenes = api.search(
        dataset='landsat_ot_c2_l1',  # 数据集名称
        latitude=34.0522,
        longitude=-118.2437,
        start_date='2020-01-01',
        end_date='2020-12-31',
        max_cloud_cover=10
    )

    print(f"{len(scenes)} scenes found.")

    # 使用线程池进行多线程下载
    download_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_scene, scene, output_dir, username, password) for scene in scenes]
        for future in as_completed(futures):
            result = future.result()
            download_results.append(result)
            print(f"Downloaded {result[0]}: {'Success' if result[1] else 'Failed'}")

    # 关闭API连接
    api.logout()

    # 保存下载结果到文件
    results_file = os.path.join(output_dir, 'download_results.txt')
    with open(results_file, 'w') as f:
        for entity_id, success in download_results:
            f.write(f"{entity_id}: {'Success' if success else 'Failed'}\n")
    print(f"Download results saved to {results_file}")


# 执行程序
if __name__ == "__main__":
    username = 'landsat8guoxiaozheng'
    password = 'N34EWUxTy2jizXR'
    output_dir = r'D:\code\landsat_download\test'
    max_workers = 4  # 设置最大线程数
    main(username, password, output_dir, max_workers)
