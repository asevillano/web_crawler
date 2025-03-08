"""
Website Crawler and File Downloader

This script crawls an initial URL using Selenium in headless mode, follows links to a specified maximum depth (or indefinitely if 0 is provided), and downloads files with the specified formats. Now, if "html" is included in the formats, the decision to download is based on checking the Content-Type (text/html) obtained via a HEAD request (or GET as a fallback), instead of just comparing the file extension in the URL.
"""

import os
import sys
import time
import argparse
import urllib.parse
import datetime
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By

from dotenv import load_dotenv

# En caso de usar el feature de Azure Blob Storage.
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# Variables globales para llevar el registro de archivos descargados y páginas visitadas
downloaded_files_count = 0
visited_pages = set()

def setup_driver(execute_js: bool):
    """
    Set up the Selenium driver in headless mode.
    Although JavaScript is not executed, the script uses Selenium.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless") # modo sin cabeza
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("--use-gl=swiftshader")
    chrome_options.add_argument("--enable-unsafe-swiftshader")

    try:  
        driver = webdriver.Chrome(options=chrome_options)  
    except WebDriverException as e:  
        print("Error setting up ChromeDriver. Make sure it is in your PATH.", e)  
        sys.exit(1)
    return driver

def upload_to_azure_blob(file_path, container_name, blob_service_client, override_if_newer=True):
    """
    Upload the file to the specified Azure Blob Storage container.  
    If a blob exists and has a modification date, it will only be uploaded if the local file is more recent.
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
        # Blob does not exist or error retrieving properties  
        pass  

    with open(file_path, "rb") as data:  
        blob_client.upload_blob(data, overwrite=True)  
    print(f"\tUploaded {file_path} to container '{container_name}'.")

def download_file(file_url, ext, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client):
    """
    Download a file from file_url to the download_dir.
    Optionally, upload the file to Azure Blob Storage if required.
    """
    global downloaded_files_count

    if file_url in exclude_download:  
        print(f"Skipping download (URL excluded): {file_url}")  
        return

    parsed = urllib.parse.urlparse(file_url)  
    filename = os.path.basename(parsed.path)  
    if not filename:  
        # If no name is found in the URL, a generic one is assigned.  
        filename = "downloaded_file_" + str(int(time.time()))  
        
    local_path = os.path.join(download_dir, filename)
    if local_path.find(ext) == -1: # If the filename has not its extension, set it
        local_path = local_path + '.' + ext
    
    try:  
        if os.path.exists(local_path):  
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

    # Optional uploading to Azure Blob Storage  
    if blob_upload and blob_service_client:  
        upload_to_azure_blob(local_path, container_name, blob_service_client)  

    time.sleep(delay)

def should_download(link, allowed_exts, timeout=10):
    """
    Determine if a resource should be downloaded based on its URL by checking:
    • If the extension (obtained from the URL) is one of the allowed ones (and not "html")
    • Or, if "html" is among the allowed extensions, a HEAD request (or fallback GET request) is made and it is checked that its Content-Type contains "text/html".
    """
    try:
        parsed = urllib.parse.urlparse(link)
        file_ext = os.path.splitext(parsed.path)[1].strip(".").lower()
    except Exception:
        file_ext = ""
    # If the URL has an allowed extension other than html, it is downloaded without additional checks. 
    if file_ext and file_ext in allowed_exts and file_ext != "html":  
        return file_ext, True  

    # If the html extension is indicated (or the URL has no extension), the Content-Type is checked.  
    if "html" in allowed_exts:  
        try:  
            head_resp = requests.head(link, allow_redirects=True, timeout=timeout)  
            content_type = head_resp.headers.get("content-type", "").lower()  
            if "text/html" in content_type:  
                return 'html', True  
        except Exception as e:  
            # If the HEAD request fails, a GET request in stream mode is attempted.  
            try:  
                get_resp = requests.get(link, stream=True, timeout=timeout)  
                content_type = get_resp.headers.get("content-type", "").lower()  
                if "text/html" in content_type:  
                    return 'html', True  
            except Exception as e2:  
                return "", False  
    return "", False  

def crawl(driver, url, start_domain, current_depth, max_depth, stay_on_domain, allowed_exts, max_files, delay, exclude_download, exclude_crawl, blob_upload, container_name, blob_service_client):
    """
    Recursive function that traverses the page at the given URL using Selenium.
    Extracts anchor tags; downloads the files if they meet the allowed extensions (in the case of "html", checking the Content-Type). Follows the links to traverse the pages.
    """
    global visited_pages, downloaded_files_count
    if max_files and downloaded_files_count >= max_files:  
        return  

    if url in visited_pages:  
        return  
    visited_pages.add(url)  
    
    # If the URL is on the exclusion list for crawling, it is skipped.  
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

    # It waits for the page to render (in case JS is executed).  
    time.sleep(delay)  
    
    # Extracts all the links (anchor tags).  
    try:  
        elems = driver.find_elements(By.TAG_NAME, "a")  
    except NoSuchElementException:  
        elems = []  
    
    links = set()  
    for elem in elems:  
        try:  
            href = elem.get_attribute("href")  
        except StaleElementReferenceException:  
            continue  # If the element is stale, it is skipped.  
        if href and href.startswith("http"):  
            links.add(href)  

    # Processes each link found  
    for link in links:  
        if max_files and downloaded_files_count >= max_files:  
            break  

        # If the resource should be downloaded (for example, if it is HTML verified by its Content-Type or an allowed extension other than html)  
        ext, down = should_download(link, allowed_exts)
        if down:
            download_file(link, ext, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client)  
        else:  
            # If it is not desired to download, it is checked whether to continue crawling  
            if stay_on_domain:  
                if urllib.parse.urlparse(link).netloc != start_domain:  
                    continue  
            if max_depth != 0 and current_depth >= max_depth:  
                continue  
            crawl(driver, link, start_domain, current_depth + 1, max_depth, stay_on_domain,  
                allowed_exts, max_files, delay, exclude_download, exclude_crawl, blob_upload, container_name, blob_service_client)  
            if max_files and downloaded_files_count >= max_files:  
                break  

def main():
    parser = argparse.ArgumentParser(description="Website crawler and file downloader")
    # Argumentos obligatorios  
    parser.add_argument("starting_url", help="The initial URL to crawl")  
    parser.add_argument("download_dir", help="Directory where the downloaded files will be saved")  
    
    # Argumentos opcionales  
    parser.add_argument("--max_depth", type=int, default=2,  
                        help="Maximum navigation depth (0 for infinite). Default: 2")  
    parser.add_argument("--js", choices=["yes", "no"], default="no",  
                        help="Execute JavaScript on the pages (Selenium headless is always used). Default: no.")  
    parser.add_argument("--stay_on_domain", choices=["yes", "no"], default="yes",  
                        help="Stay on the same domain as the initial URL. Default: yes.")  
    parser.add_argument("--max_files", type=int, default=100,  
                        help="Maximum number of files to download. Default: 100.")  
    parser.add_argument("--extensions", nargs="+", default=["pdf", "html"],  
                        help="File extensions to download (without the dot). Default: pdf html.")  
    parser.add_argument("--delay", type=float, default=2,  
                        help="Seconds to wait between requests. Default: 2.")  
    parser.add_argument("--exclude_download", nargs="*", default=[],  
                        help="List of URL substrings to exclude from downloads.")  
    parser.add_argument("--exclude_crawl", nargs="*", default=[],  
                        help="List of URL substrings to exclude from crawling.")  
    parser.add_argument("--upload_blob", choices=["yes", "no"], default="no",  
                        help="Upload files to Azure Blob Storage. Default: no.")  
    parser.add_argument("--container", help="Name of the container in Azure Blob Storage (required if --upload_blob yes).")  

    args = parser.parse_args()  

    global download_dir  # used in crawl() and download_file()  
    download_dir = args.download_dir  

    # Create the download directory if it does not exist  
    if not os.path.exists(download_dir):  
        os.makedirs(download_dir)  

    # Configura Azure Blob Storage si es necesario.  
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
        load_dotenv(override=True)  
        conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")  
        if not conn_str:  
            print("Error: AZURE_BLOB_CONNECTION_STRING not found in .env file.")  
            sys.exit(1)  
        try:  
            blob_service_client = BlobServiceClient.from_connection_string(conn_str)  
            try:  
                blob_service_client.create_container(container_name)  
                print(f"Created container: {container_name}")  
            except Exception:  
                pass  
        except Exception as e:  
            print("Error setting up Azure Blob Storage:", e)  
            sys.exit(1)  

    # Inicializa el Selenium WebDriver  
    driver = setup_driver(args.js.lower() == "yes")  
    start_domain = urllib.parse.urlparse(args.starting_url).netloc  

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