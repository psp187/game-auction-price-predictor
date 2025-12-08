import requests
from logger_config import setup_logger
from pathlib import Path
from utils import export_to_json

class DataCollector:
    def __init__(self, api_url:str, output_dir_path: Path, output_filename: str, last_update_file: Path, logger_conf:tuple):
        self.api_url = api_url
        self.output_dir_path = output_dir_path
        self.output_filename = output_filename #only string name, without suffix
        self.last_update_file = last_update_file
        self.logger = setup_logger(*logger_conf) #* needed to unpack tuple

    def fetch_data(self):
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched data. Status code: {response.status_code}")
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error while fetching data: {e}")
            return None

    def last_update(self, data: dict) -> bool:
        try:
            timestamp = data.get('lastUpdated')
            if not self.last_update_file.exists():
                self.last_update_file.write_text(str(timestamp))
                return True
            else:
                time = int(self.last_update_file.read_text())
                if time != timestamp:
                    self.last_update_file.write_text(str(timestamp))
                    return True
                else:
                    return False
        except Exception as e:
            self.logger.error(f"Error while checking last update: {e}")
            return False

    def fetch_new(self):
        self.logger.info("Starting data collection run.")
        data = self.fetch_data()

        if not data:
            self.logger.error("No data found")
            return None
        if self.last_update(data):
            self.logger.info("New data found.")
            if data.get("success"):
                return export_to_json(
                    data,
                    file_dir_path=self.output_dir_path,
                    file_name=self.output_filename,
                    create_folder=True,
                    time_stamp=True
                )
            else:
                cause = data.get('cause', 'No cause provided')
                self.logger.warning(f"API call was not successful. {cause}")
        else:
            self.logger.info("No new data found. Skipping file creation.")

        self.logger.info("--- Data collection run finished ---")

