import threading
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path
import time

# Authentication credentials
username = 'guoxiaozheng123'
password = 'ybqLDijfFCbP4m4'

# Max number of retries for downloading
MAX_RETRIES = 3
RETRY_DELAY = 5  # Delay between retries in seconds

# Function to download a file with retries
def download_file(url, session, download_dir):
    local_filename = download_dir / Path(url).name

    if local_filename.exists():
        print(f"File already exists, skipping: {local_filename}")
        return

    retries = 0
    while retries < MAX_RETRIES:
        try:
            with session.get(url) as response:
                response.raise_for_status()  # Ensure we notice bad responses
                with open(local_filename, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {local_filename}")
                return  # Exit once download is successful
        except (requests.RequestException, Exception) as e:
            retries += 1
            print(f"Error downloading {url}: {e}. Retrying {retries}/{MAX_RETRIES}...")
            if retries == MAX_RETRIES:
                print(f"Failed to download {url} after {MAX_RETRIES} retries.")
                # Optionally, log the failed URLs to a file for later review
                with open('failed_downloads.txt', 'a') as log_file:
                    log_file.write(f"{url}\n")
            time.sleep(RETRY_DELAY)

# Divide the list of URLs among a number of threads
def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Main function to download files using multiple threads
def main(download_links, num_threads=6):
    download_dir = Path(r'F:\SHOP\20241219\50\data')
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
file_path = r'F:\SHOP\20241219\50\4832306649-download.txt'  # Change this to your file path
with open(file_path, 'r') as file:
    urls = file.read().splitlines()

main(urls)
