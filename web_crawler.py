# Web Crawler and File Downloader

# This script crawls a starting URL using Selenium in headless mode, follows links up to a maximum depth (or infinitely if 0), 
# #and downloads files with the specified file extensions.

# You can specify the maximum depth (number of levels within the website hierarchy to follow), execute JavaScript, 
# set the maximum number of files to download, restrict crawling to the same domain, specify file extensions to download, 
# configure a wait time between requests, exclude specific URLs from downloading and crawling, 
# and optionally upload the downloaded files to Azure Blob Storage (only if the downloaded file is newer).
                                                                                                                                                                                                                                                                                                                                                                                                        
import os
import sys
import time
import argparse
import urllib.parse
import datetime
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from selenium.webdriver.common.by import By

from dotenv import load_dotenv

# If you intend to use the Azure Blob Storage feature, make sure to install the package.
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# Global variables to keep track of downloaded files and visited URLs
downloaded_files_count = 0
visited_pages = set()

def setup_driver(execute_js: bool):
    """
    Set up the Selenium web driver in headless mode.
    If JS execution is not required, the script still uses Selenium.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # run headless
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    
    # You can add additional Chrome options if needed.
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except WebDriverException as e:
        print("Error setting up ChromeDriver. Ensure it is in your PATH.", e)
        sys.exit(1)
    return driver

def upload_to_azure_blob(file_path, container_name, blob_service_client, override_if_newer=True):
    """
    Upload the file to the specified Azure Blob Storage container.
    Check if a blob exists with a last-modified date, and upload only if the local file is newer.
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(file_path))
    
    try:
        props = blob_client.get_blob_properties()
        blob_last_modified = props.last_modified.replace(tzinfo=None)
        local_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if local_modified <= blob_last_modified:
            print(f"Skipping upload for {file_path} since blob is up-to-date.")
            return
    except Exception:
        # Blob does not exist or error in retrieving its properties
        pass

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"\tUploaded {file_path} to container '{container_name}'.")

def download_file(file_url, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client):
    """
    Downloads a file from file_url into the download_dir.
    Then, optionally, uploads the file to Azure Blob Storage if required.
    """
    global downloaded_files_count

    parsed = urllib.parse.urlparse(file_url)
    if file_url in exclude_download:
        print(f"Skipping download (URL excluded): {file_url}")
        return

    filename = os.path.basename(parsed.path)
    if not filename:
        filename = "downloaded_file_" + str(int(time.time()))
        
    local_path = os.path.join(download_dir, filename)
    
    try:
        if os.path.exists(local_path): # Añadido ASC
            print(f"\tFile {local_path} already exists, skipping it")
        else:
            response = requests.get(file_url, stream=True, timeout=15)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"\tDownloaded: {file_url} -> {local_path}")
            downloaded_files_count += 1
    except Exception as e:
        print(f"\tError downloading {file_url}: {e}")
        return

    # Optional Azure Blob Storage upload
    if blob_upload and blob_service_client:
        upload_to_azure_blob(local_path, container_name, blob_service_client)

    time.sleep(delay)


def crawl(driver, url, start_domain, current_depth, max_depth, stay_on_domain,
          allowed_exts, max_files, delay, exclude_download, exclude_crawl,
          blob_upload, container_name, blob_service_client):
    """
    Recursively crawl the page at url using Selenium.
    Extracts anchor tags; downloads files if they have allowed extensions.
    Follows links to traverse pages.
    """
    global visited_pages, downloaded_files_count

    if max_files and downloaded_files_count >= max_files:
        return

    if url in visited_pages:
        return
    visited_pages.add(url)
    
    # If the URL is in the EXCLUDE crawl list, skip it.
    for ex in exclude_crawl:
        if ex in url:
            print(f"Skipping crawl (URL excluded): {url}")
            return

    print(f"Crawling (depth {current_depth}): {url}")
    try:
        driver.get(url)
    except Exception as e:
        print(f"Error loading {url}: {e}")
        return

    # Give time for page rendering (if JS required)
    time.sleep(delay)
    
    # Extract link elements
    try:
        elems = driver.find_elements(By.TAG_NAME, "a")
    except NoSuchElementException:
        elems = []
    
    links = set()
    for elem in elems:
        href = elem.get_attribute("href")
        if href and href.startswith("http"):
            links.add(href)

    # Process each found link
    for link in links:
        # First, check if link points to a file with the allowed extension
        parsed_link = urllib.parse.urlparse(link)
        file_ext = os.path.splitext(parsed_link.path)[1].strip(".").lower()
        if file_ext in allowed_exts:
            # Download file if we haven't reached limit yet
            if max_files and downloaded_files_count >= max_files:
                break
            download_file(link, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client)
        else:
            # Check if we should crawl further
            # Stay on same domain if required
            if stay_on_domain:
                if urllib.parse.urlparse(link).netloc != start_domain:
                    continue
            # Continue recursing if maximum depth not reached
            if max_depth != 0 and current_depth >= max_depth:
                continue
            # Recurse further
            crawl(driver, link, start_domain, current_depth + 1, max_depth, stay_on_domain,
                  allowed_exts, max_files, delay, exclude_download, exclude_crawl, blob_upload, container_name, blob_service_client)
            if max_files and downloaded_files_count >= max_files:
                break

def main():
    parser = argparse.ArgumentParser(description="Website crawler and file downloader")
    
    # Mandatory arguments
    parser.add_argument("starting_url", help="The starting URL to crawl")
    parser.add_argument("download_dir", help="Directory where downloaded files are stored")
    
    # Optional arguments
    parser.add_argument("--max_depth", type=int, default=2,
                        help="Maximum depth of navigation (0 for infinite). Default: 2")
    parser.add_argument("--js", choices=["yes", "no"], default="no",
                        help="Execute JavaScript on pages (using Selenium headless always; this flag might affect waiting times). Default: no")
    parser.add_argument("--stay_on_domain", choices=["yes", "no"], default="yes",
                        help="Stay on the same domain as the starting URL. Default: yes")
    parser.add_argument("--max_files", type=int, default=100,
                        help="Maximum number of files to download. Default: 100")
    parser.add_argument("--extensions", nargs="+", default=["pdf", "html"],
                        help="File extensions to download (without the dot). Default: pdf html")
    parser.add_argument("--delay", type=float, default=1,
                        help="Number of seconds to wait between requests. Default: 1")
    parser.add_argument("--exclude_download", nargs="*", default=[],
                        help="List of URLs substrings to exclude from downloading")
    parser.add_argument("--exclude_crawl", nargs="*", default=[],
                        help="List of URLs substrings to exclude from crawling")
    parser.add_argument("--upload_blob", choices=["yes", "no"], default="no",
                        help="Upload files to Azure Blob Storage. Default: no")
    parser.add_argument("--container", help="Azure Blob Storage container name (required if --upload_blob yes)")

    args = parser.parse_args()

    global download_dir  # to be used in crawl() and download_file()
    download_dir = args.download_dir

    # Create download directory if it does not exist.
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Prepare Azure Blob Storage if required.
    blob_upload = args.upload_blob.lower() == "yes"
    container_name = None
    blob_service_client = None
    if blob_upload:
        if not args.container:
            print("Error: Please specify --container <container_name> when --upload_blob is yes.")
            sys.exit(1)
        container_name = args.container
        if BlobServiceClient is None:
            print("Error: azure-storage-blob package is not installed. Use pip install azure-storage-blob")
            sys.exit(1)
        # Load connection string from .env file
        load_dotenv(override=True)
        conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")
        if not conn_str:
            print("Error: AZURE_BLOB_CONNECTION_STRING not found in .env file.")
            sys.exit(1)
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            # Optionally create container if it doesn't exist.
            try:
                blob_service_client.create_container(container_name)
                print(f"Created container: {container_name}")
            except Exception:
                pass
        except Exception as e:
            print("Error setting up Azure Blob Storage:", e)
            sys.exit(1)

    # Set up Selenium WebDriver
    driver = setup_driver(args.js.lower() == "yes")
    start_domain = urllib.parse.urlparse(args.starting_url).netloc

    # Start crawling from the starting URL
    try:
        crawl(driver=driver,
              url=args.starting_url,
              start_domain=start_domain,
              current_depth=1,
              max_depth=args.max_depth,
              stay_on_domain=args.stay_on_domain.lower() == "yes",
              allowed_exts=[ext.lower() for ext in args.extensions],
              max_files=args.max_files,
              delay=args.delay,
              exclude_download=args.exclude_download,
              exclude_crawl=args.exclude_crawl,
              blob_upload=blob_upload,
              container_name=container_name,
              blob_service_client=blob_service_client)
    finally:
        driver.quit()
    
    print(f"Finished crawling. Total files downloaded: {downloaded_files_count}")

if __name__ == "__main__":
    main()