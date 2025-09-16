import requests
from logger_config import setup_logger
import json
import datetime
from pathlib import Path

API_URl = r"https://api.hypixel.net/v2/skyblock/auctions_ended"
OUTPUT_FOLDER = "raw_data"

logger = setup_logger("data_collector", "data_collector.log")

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

def last_update(data, txt_file = Path('last_update.txt')) -> bool:
    try:
        timestamp = data.get('lastUpdated')
        if not txt_file.exists():
            txt_file.write_text(str(timestamp))
            return True
        else:
            time = int(txt_file.read_text())
            if time != timestamp:
                txt_file.write_text(str(timestamp))
                return True
            else:
                return False
    except Exception as e:
        logger.error(f"Error while fetching data: {e}")
        return False


def main():
    logger.info("--- Starting data collection run ---")
    data = fetch_data(API_URl)

    flag = last_update(data)

    if not data:
        logger.error("No data found")
        return
    if flag:
        logger.info("New data found.")
        if data.get("success"):
            export_to_json(data, folder=OUTPUT_FOLDER)
        else:
            cause = data.get('cause', 'No cause provided')
            logger.warning(f"API call was not successful. {cause}")
    else:
        logger.info("No new data found. Skipping file creation.")

    logger.info("--- Data collection run finished ---")

if __name__ == "__main__":
    main()