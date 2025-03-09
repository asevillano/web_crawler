"""
Web Crawler and File Downloader

Esta versión recorre (crawl) una URL de partida usando Selenium (modo headless),
descarga los ficheros cuyas extensiones se indiquen y además sigue los enlaces
de las páginas HTML. Si una URL no tiene extensión se asume que es HTML.
La profundidad de rastreo se calcula en función del número de saltos (nivel 1 es la URL de partida, 2 es una página enlazada, etc.).
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
from azure.storage.blob import BlobServiceClient

# Variables globales para llevar conteo de archivos descargados y URLs visitadas.
downloaded_files_count = 0
visited_pages = set()

def setup_driver(execute_js: bool):
    """
    Configura el driver de Selenium en modo headless.
    Aunque no se ejecute JavaScript, se utiliza Selenium.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless") # modo headless
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("--use-gl=swiftshader")
    chrome_options.add_argument("--enable-unsafe-swiftshader")

    try:  
        driver = webdriver.Chrome(options=chrome_options)  
    except WebDriverException as e:  
        print("Error configurando ChromeDriver. Asegúrate de que esté en tu PATH.", e)  
        sys.exit(1)  
    return driver  
 
def upload_to_azure_blob(file_path, container_name, blob_service_client, override_if_newer=True):
    """
    Sube un fichero al contenedor especificado en Azure Blob Storage.
    Verifica la fecha de última modificación y sube el archivo únicamente si es más reciente.
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
        # El blob no existe o hubo error en la obtención de sus propiedades  
        pass  

    with open(file_path, "rb") as data:  
        blob_client.upload_blob(data, overwrite=True)  
    print(f"\tUploaded {file_path} to container '{container_name}'.")  

def download_file(file_url, file_ext, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client):
    """
    Descarga el fichero desde file_url hacia download_dir.
    Si se trata de una página HTML se extrae la última parte de la URL y se le asigna la extensión ".html".
    Por ejemplo, si la URL es "https://www.website.com/page1/", se guardará como "page1.html".
    Posteriormente, opcionalmente se sube el archivo a Azure Blob Storage.
    """
    global downloaded_files_count

    parsed = urllib.parse.urlparse(file_url)  
    if file_url in exclude_download:  
        print(f"Skipping download (URL excluded): {file_url}")  
        return  

    # Para la asignación del nombre del archivo:  
    filename = os.path.basename(parsed.path)  
    if file_ext == "html":  
        if not filename:  
            # Si os.path.basename() es vacío (caso de terminar en slash) se extrae la última parte no vacía.  
            parts = parsed.path.strip('/').split('/')  
            if parts and parts[-1]:  
                filename = parts[-1] + ".html"  
            else:  
                # Si no se pudo obtener ninguna parte (por ejemplo, la URL es solo el dominio), se asigna "home.html"  
                filename = "home.html"  
        else:  
            # Si se obtuvo un nombre (por ejemplo, "page1") y no contiene extensión, se le agrega ".html"  
            if not (filename.lower().endswith(".html") or filename.lower().endswith(".htm")):  
                filename += ".html"  
    else:  
        if not filename:  
            filename = "downloaded_file_" + str(int(time.time())) + "." + file_ext  

    local_path = os.path.join(download_dir, filename)  
    
    if os.path.exists(local_path):  
        print(f"\tFile {local_path} already exists, skipping it")  
        return  

    try:  
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

    # Subida opcional a Azure Blob Storage  
    if blob_upload and blob_service_client:  
        upload_to_azure_blob(local_path, container_name, blob_service_client)  

    time.sleep(delay)  
 
def crawl(driver, url, start_domain, current_depth, max_depth, stay_on_domain,
allowed_exts, max_files, delay, exclude_download, exclude_crawl,
blob_upload, container_name, blob_service_client):
    """
    Recorre (crawl) recursivamente la página en la URL dada usando Selenium.
    Extrae los enlaces (“a”) y, dependiendo de la extensión:
    • Si la URL no tiene extensión se asume HTML.
    • Si la URL es una página HTML (extensión "html") se descarga (si se ha solicitado)
    y se recorre el contenido para extraer más enlaces (si se cumple la profundidad).
    • Si la URL tiene una extensión en allowed_exts (como PDF, etc.) se descarga.
    • En caso de que la extensión no esté en allowed_exts se asume que es una página
    (por ejemplo, “.php”) y se recorre.
    La profundidad se cuenta como el número de “saltos” entre enlaces (nivel 1: URL inicial, 2: enlace, etc.).
    """
    global visited_pages, downloaded_files_count

    if max_files and downloaded_files_count >= max_files:  
        return  

    if url in visited_pages:  
        return  
    visited_pages.add(url)  
    
    # Si la URL se encuentra en la lista de exclusión para el crawling se omite.  
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

    # Dar tiempo para que se renderice la página (en caso de requerir JS)  
    time.sleep(delay)  
    
    # Extraer todos los enlaces de la página  
    try:  
        elems = driver.find_elements(By.TAG_NAME, "a")  
    except NoSuchElementException:  
        elems = []  
    
    # Prepare the list of found links
    links = set()  
    for elem in elems:  
        try:  
            href = elem.get_attribute("href")  
        except StaleElementReferenceException:  
            continue  # Si el elemento es stale, se omite  
        if href and href.startswith("http"):  
            links.add(href)  
    
    # Remove visited pages from links found
    links.difference_update(visited_pages)
    print(f"\tfound links after removing visited: {links}")

    # Procesamos cadta enlace encontrado  
    for link in links:  
        parsed_link = urllib.parse.urlparse(link)  
        # Extrae la extensión: si no hay (cadena vacía) se asume HTML  
        file_ext = os.path.splitext(parsed_link.path)[1].strip(".").lower()  
        if file_ext == "":  
            file_ext = "html"  

        # Tratamiento especial para páginas HTML  
        if file_ext == "html":  
            # Si se restringe a un dominio, verificar que el enlace pertenezca al mismo.  
            if stay_on_domain and urllib.parse.urlparse(link).netloc != start_domain:  
                continue  
            # Si se indicó que se descarguen páginas HTML (por ejemplo, al incluir "html" en --extensions)  
            if "html" in allowed_exts and (max_files == 0 or downloaded_files_count < max_files):  
                download_file(link, "html", download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client)  
            # Si se supera la profundidad máxima, no se continúa.  
            if max_depth != 0 and current_depth >= max_depth:  
                continue  
            # Recorrida recursiva en la página HTML  
            crawl(driver, link, start_domain, current_depth + 1, max_depth, stay_on_domain,  
                allowed_exts, max_files, delay, exclude_download, exclude_crawl, blob_upload, container_name, blob_service_client)  
            if max_files and downloaded_files_count >= max_files:  
                break  

        # Si la extensión está en la lista de archivos a descargar (por ejemplo, pdf) y NO es html.  
        elif file_ext in allowed_exts:  
            if max_files and downloaded_files_count >= max_files:  
                break  
            download_file(link, file_ext, download_dir, delay, exclude_download, blob_upload, container_name, blob_service_client)  

        # En caso de que la extensión no esté en allowed_exts se asume que podría tratarse de una página (por ejemplo, .php)  
        else:  
            if stay_on_domain and urllib.parse.urlparse(link).netloc != start_domain:  
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
    parser.add_argument("starting_url", help="La URL de partida para el rastreo")  
    parser.add_argument("download_dir", help="Directorio donde se almacenan los archivos descargados")  
    
    # Argumentos opcionales  
    parser.add_argument("--max_depth", type=int, default=2,  
                        help="Profundidad máxima de navegación (0 para infinita). Por defecto: 2")  
    parser.add_argument("--js", choices=["yes", "no"], default="no",  
                        help="Ejecutar JavaScript en las páginas (usando Selenium headless; puede afectar los tiempos de espera). Por defecto: no")  
    parser.add_argument("--stay_on_domain", choices=["yes", "no"], default="yes",  
                        help="Restringir el rastreo al mismo dominio que la URL de partida. Por defecto: yes")  
    parser.add_argument("--max_files", type=int, default=100,  
                        help="Número máximo de archivos a descargar. Por defecto: 100")  
    parser.add_argument("--extensions", nargs="+", default=["pdf", "html"],  
                        help="Extensiones de archivo a descargar (sin el punto). Por defecto: pdf html")  
    parser.add_argument("--delay", type=float, default=1,  
                        help="Segundos a esperar entre solicitudes. Por defecto: 1")  
    parser.add_argument("--exclude_download", nargs="*", default=[],  
                        help="Lista de subcadenas de URL a excluir de la descarga")  
    parser.add_argument("--exclude_crawl", nargs="*", default=[],  
                        help="Lista de subcadenas de URL a excluir del rastreo")  
    parser.add_argument("--upload_blob", choices=["yes", "no"], default="no",  
                        help="Subir archivos a Azure Blob Storage. Por defecto: no")  
    parser.add_argument("--container", help="Nombre del contenedor de Azure Blob Storage (requerido si --upload_blob yes)")  

    args = parser.parse_args()  

    global download_dir  # para usarlo en crawl() y download_file()  
    download_dir = args.download_dir  

    # Crear el directorio de descarga si no existe.  
    if not os.path.exists(download_dir):  
        os.makedirs(download_dir)  

    # Configurar Azure Blob Storage si es necesario.  
    blob_upload = args.upload_blob.lower() == "yes"  
    container_name = None  
    blob_service_client = None  
    if blob_upload:  
        if not args.container:  
            print("Error: Especifique --container <container_name> cuando --upload_blob sea yes.")  
            sys.exit(1)  
        container_name = args.container  
        if BlobServiceClient is None:  
            print("Error: El paquete azure-storage-blob no está instalado. Use pip install azure-storage-blob")  
            sys.exit(1)  
        load_dotenv(override=True)  
        conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")  
        if not conn_str:  
            print("Error: AZURE_BLOB_CONNECTION_STRING no se encontró en el archivo .env.")  
            sys.exit(1)  
        try:  
            blob_service_client = BlobServiceClient.from_connection_string(conn_str)  
            try:  
                blob_service_client.create_container(container_name)  
                print(f"Created container: {container_name}")  
            except Exception:  
                pass  
        except Exception as e:  
            print("Error configurando Azure Blob Storage:", e)  
            sys.exit(1)  

    # Configurar Selenium WebDriver  
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