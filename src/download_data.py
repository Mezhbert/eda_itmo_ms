import os
import requests
import yaml
import logging
from tqdm import tqdm

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)

def download_file(url, dest_path):
    r = requests.get(url, stream=True)
    r.raise_for_status()
    total_size = int(r.headers.get('content-length', 0))
    with open(dest_path, 'wb') as f, tqdm(
        total=total_size, unit='B', unit_scale=True, desc=f"Downloading {os.path.basename(dest_path)}"
    ) as pbar:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))
    logging.info(f"Downloaded: {dest_path}")

def main():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error reading config.yaml: {e}")
        return
    
    if not config.get("download_data", False):
        logging.info("download_data=False или отсутствует в config.yaml. Загрузка данных не требуется.")
        return
    
    data_dir = config.get('data_dir', 'data')
    os.makedirs(data_dir, exist_ok=True)

    files = {}
    if 'links' in config and isinstance(config['links'], dict):
        files.update(config['links'])
    else:
        logging.warning("No 'links' section found in config.yaml or it is not a dictionary.")
        return

    if not files:
        logging.warning("Нет файлов для скачивания в config.yaml!")
        return

    for name, url in files.items():
        filename = f"{name}.parquet"
        dest_path = os.path.join(data_dir, filename)
        try:
            download_file(url, dest_path)
        except Exception as e:
            logging.error(f"Ошибка загрузки {url}: {e}")

if __name__ == "__main__":
    main()
