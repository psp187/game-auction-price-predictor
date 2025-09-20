import requests
from logger_config import setup_logger
import json
import datetime
from pathlib import Path

API_URl = r"https://api.hypixel.net/v2/skyblock/auctions_ended"
OUTPUT_FOLDER = "raw_data"
logger = setup_logger("data_collector", "data_collector.log")

def fetch_data(url:str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"Successfully fetched data. Status code: {response.status_code}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error while fetching data: {e}")
        return None

def export_to_json(data, filename: str = "auctions", folder: str = OUTPUT_FOLDER) -> Path:
    t = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    folder_path = Path(folder)
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / f"{filename}_{t}.json"

    logger.info(f"Exporting data to {file_path}")

    with file_path.open( "w", encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info("Data exported successfully.")

    return file_path

def last_update(data: dict, txt_file: Path = Path('last_update.txt')) -> bool:
    timestamp = data.get('lastUpdated')
    if timestamp is None:
        logger.warning("No 'lastUpdated' in data")
        return False

    current = int(txt_file.read_text()) if txt_file.exists() else None
    if current != timestamp:
        txt_file.write_text(str(timestamp))
        return True
    return False

def fetch_new() -> Path | None:
    logger.info("--- Starting data collection run ---")
    data = fetch_data(API_URl)
    if not data:
        logger.error("No data found")
        return None

    if not last_update(data):
        logger.info("No new data found. Skipping file creation.")
        return None

    logger.info("New data found.")
    if data.get("success"):
        return export_to_json(data)

    cause = data.get('cause', 'No cause provided')
    logger.warning(f"API call was not successful. {cause}")
    logger.info("--- Data collection run finished ---")
    return None

def main():
    fetch_new()

if __name__ == "__main__":
    main()