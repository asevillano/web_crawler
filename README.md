# Web Crawler and File Downloader

This script crawls a starting URL using Selenium in headless mode, follows links up to a maximum depth (or infinitely if 0), and downloads files with the specified file extensions.

You can specify to execute JavaScript or not, set the maximum number of files to download, restrict crawling to the same domain, configure a wait time between requests, exclude specific URLs from downloading and crawling, and optionally upload the downloaded files to Azure Blob Storage (only if the downloaded file is newer).

**Usage:**

  python web_crawler.py <starting_url> <download_directory>

         [--max_depth MAX_DEPTH]
         [--js {yes,no}]
         [--stay_on_domain {yes,no}]
         [--max_files MAX_FILES]
         [--extensions EXT [EXT ...]]
         [--delay SECONDS]
         [--exclude_download EX_URL [EX_URL ...]]
         [--exclude_crawl EX_URL [EX_URL ...]]
         [--upload_blob {yes,no}]
         [--container CONTAINER_NAME]
         
For Azure Blob Storage uploading, the script expects an Azure Blob connection string in a .env file:

  AZURE_BLOB_CONNECTION_STRING=<your_connection_string>
  
Example:

  python web_crawler.py "https://example.com" "./downloads" --max_depth 3 --js yes --extensions pdf html docx png jpg --upload_blob yes --container mycontainer


Requirements:

  - Python 3.6+
  - Selenium (pip install selenium)
  - Requests (pip install requests)
  - python-dotenv (pip install python-dotenv)
  - (Optional) Azure Blob Storage SDK (pip install azure-storage-blob)
  - A compatible WebDriver (e.g., [ChromeDriver](https://developer.chrome.com/docs/chromedriver/downloads) ) available in your PATH.