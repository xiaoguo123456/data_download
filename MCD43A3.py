import threading
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path

# Authentication credentials
username = 'guoxiaozheng123'
password = 'ybqLDijfFCbP4m4'


# Function to download a file
def download_file(url, session, download_dir):
    local_filename = download_dir / Path(url).name
    with session.get(url) as response:
        response.raise_for_status()  # Ensure we notice bad responses
        with open(local_filename, 'wb') as f:
            f.write(response.content)
        print(f'Downloaded {local_filename}')


# Divide the list of URLs among a number of threads
def divide_chunks(l, n):
    # Looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


# Main function to download files using multiple threads
def main(download_links, num_threads=6):
    download_dir = Path(r'E:\shop\20240322\MCD43A3')
    download_dir.mkdir(parents=True, exist_ok=True)

    threads = []
    for chunk in divide_chunks(download_links,
                               int(len(download_links) / num_threads + (len(download_links) % num_threads > 0))):
        session = requests.Session()
        session.auth = HTTPBasicAuth(username, password)
        thread = threading.Thread(target=lambda: [download_file(url, session, download_dir) for url in chunk])
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


# Load the download links from a file
file_path = r'E:\shop\20240322\MCD43A3-download.txt'  # Change this to your file path
with open(file_path, 'r') as file:
    urls = file.read().splitlines()

main(urls)
