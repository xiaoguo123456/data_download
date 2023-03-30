import json
import requests
import sys
import time
import os
import re
import threading
import datetime
import pandas as pd

os.environ["http_proxy"] = "http://127.0.0.1:10810"
os.environ["https_proxy"] = "http://127.0.0.1:10810"

path = r"F:\github\dowload_landsat\data"  # Fill a valid download path
maxthreads = 5  # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  # Customized label using date time
threads = []




# Send http request
def sendRequest(url, data, apiKey=None, exitIfNoResponse=True):
    json_data = json.dumps(data)

    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}
        response = requests.post(url, json_data, headers=headers)

    try:
        httpStatusCode = response.status_code
        if response == None:
            print("No output from service")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        output = json.loads(response.text)
        if output['errorCode'] != None:
            print(output['errorCode'], "- ", output['errorMessage'])
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        if httpStatusCode == 404:
            print("404 Not Found")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 401:
            print("401 Unauthorized")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 400:
            print("Error Code", httpStatusCode)
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
    except Exception as e:
        response.close()
        print(e)
        if exitIfNoResponse:
            sys.exit()
        else:
            return False
    response.close()

    return output['data']


def downloadFile(url):
    sema.acquire()
    try:
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        if path != "" and path[-1] != "/":
            filename = "/" + filename
        open(path + filename, 'wb').write(response.content)
        print(f"Downloaded {filename}\n")
        sema.release()
    except Exception as e:
        print(f"Failed to download from {url}. Will try to re-download.")
        sema.release()
        runDownload(threads, url)


def runDownload(threads, url):
    thread = threading.Thread(target=downloadFile, args=(url,))
    threads.append(thread)
    thread.start()


if __name__ == '__main__':

    username = 'landsat8guoxiaozheng'
    password = 'N34EWUxTy2jizXR'
    filetype = 'bundle'

    print("\nRunning Scripts...\n")
    startTime = time.time()

    serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"

    # Login
    payload = {'username': username, 'password': password}
    apiKey = sendRequest(serviceUrl + "login", payload)
    print("API Key: " + apiKey + "\n")

    # Read scenes
    f = pd.read_excel(r'F:\github\dowload_landsat\all.xlsx')

    datasetName = 'landsat_ot_c2_l1'


    print("Scenes details:")
    print(f"Dataset name: {datasetName}")

    entityIds = list(f['Landsat Sc'].values)


    listId = f"temp_{datasetName}_list"  # customized list id


    # Get download options
    payload = {
        "listId": listId,
        "entityIds": entityIds,
        "datasetName": datasetName
    }

    print("Getting product download options...\n")
    products = sendRequest(serviceUrl + "download-options", payload, apiKey)
    print("Got product download options\n")


    # Select products
    downloads = []

    # select bundle files
    for product in products:
        if product["bulkAvailable"]:
            if 'Bundle' in product["productName"]:
                downloads.append({"entityId": product["entityId"], "productId": product["id"]})

    # Send download-request
    payLoad = {
        "downloads": downloads,
        "label": label,
        'returnAvailable': True
    }

    print(f"Sending download request ...\n")
    results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
    print(f"Done sending download request\n")
    save_data=[]
    for result in results['availableDownloads']:
        print(f"Get download url: {result['url']}\n")
        runDownload(threads, result['url'])
        save_data.append(result['url'])
    save_data=pd.DataFrame({'url':save_data})
    save_data.to_excel('out.xls')
    preparingDownloadCount = len(results['preparingDownloads'])
    preparingDownloadIds = []
    if preparingDownloadCount > 0:
        for result in results['preparingDownloads']:
            preparingDownloadIds.append(result['downloadId'])

        payload = {"label": label,
                   "downloadApplication": "BulkDownload"}
        # Retrieve download urls
        print("Retrieving download urls...\n")
        results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
        if results != False:
            for result in results['available']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n")
                    runDownload(threads, result['url'])

            for result in results['requested']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n")
                    runDownload(threads, result['url'])

        # Don't get all download urls, retrieve again after 30 seconds
        while len(preparingDownloadIds) > 0:
            print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
            time.sleep(30)
            results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
            if results != False:
                for result in results['available']:
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        print(f"Get download url: {result['url']}\n")
                        runDownload(threads, result['url'])

    print("\nGot download urls for all downloads\n")
    # Logout
    endpoint = "logout"
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
        print("Logged Out\n")
    else:
        print("Logout Failed\n")

    print("Downloading files... Please do not close the program\n")
    for thread in threads:
        thread.join()

    print("Complete Downloading")

    executionTime = round((time.time() - startTime), 2)
    print(f'Total time: {executionTime} seconds')