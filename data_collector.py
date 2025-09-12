from logging.handlers import RotatingFileHandler
import requests
import logging
import json
import datetime
from pathlib import Path

API_URl = r"https://api.hypixel.net/v2/skyblock/auctions_ended"
OUTPUT_FOLDER = "raw_data"

log_dir = Path(".") / "logs"
log_file = log_dir / f"data_collector.log"
log_dir.mkdir(parents=True, exist_ok=True)
handler = RotatingFileHandler(log_file, maxBytes=5242880, backupCount=5, encoding="utf-8")

log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(log_format)

logger = logging.getLogger("data_collector")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"Successfully fetched data. Status code: {response.status_code}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error while fetching data: {e}")
        return None


def export_to_json(data, filename = "auctions", folder=OUTPUT_FOLDER):
    t = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    folder_path = Path(folder)
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / f"{filename}_{t}.json"

    logger.info(f"Exporting data to {file_path}")

    with file_path.open( "w", encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info("Data exported successfully.")


def main():
    logger.info("--- Starting data collection run ---")
    data = fetch_data(API_URl)

    if not data:
        logger.error("No data found")
        return

    if data.get("success"):
        export_to_json(data, folder=OUTPUT_FOLDER)

    else:
        cause = data.get('cause', 'No cause provided')
        logger.warning(f"API call was not successful. {cause}")

    logger.info("--- Data collection run finished ---")

if __name__ == "__main__":
    main()