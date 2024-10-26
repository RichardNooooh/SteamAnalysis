import logging
import time
from api_scraper import APIScraper
import json
import os

class GamalyticScraper(APIScraper):
    def __init__(self):
        super().__init__("https://api.gamalytic.com/game/")
        self.id_file = "../../data/raw/steam_ids/game_ids.txt"
        self.data_file = "../../data/raw/gamalytic/data"
        self.REQUEST_INTERVAL_TIME = 0.25

        self.GAMALYTIC_API_KEY = os.getenv("GAMALYTIC_API_KEY")
        self.log = logging.getLogger(__name__)

        self.params = {
            "include_pre_release_history": "true",
        }
        self.headers = {
            "api-key": self.GAMALYTIC_API_KEY
        }


    def get_data(self, start: int = 0, limit: int = 10000):
        """
        Fetches all data for a range of IDs.
        Args:
            start (int): Starting index of the IDs.
            limit (int): Number of IDs to process.
        """
        self.log.info("Beginning data retrieval from Gamalytics")
        self.log.info(f"Reading app IDs from {self.id_file}")
        app_ids = []

        # Read the app IDs from the file
        with open(self.id_file, 'r') as f:
            for app in f:
                data = app.split("\t")
                if len(data) == 1:
                    continue
                app_ids.append((int(data[0]), data[1].strip()))

        app_ids = app_ids[start:start + limit]
        end = start + len(app_ids) - 1

        self.log.info(f"Starting API scraping for {len(app_ids)} apps (from index {start} to {end})")

        file_name = f"{self.data_file}_{start}_{end}.jsonl"
        self.log.info(f"Saving data to {file_name}")
        with open(file_name, mode="w") as output_file:
            for i, (app_id, app_name) in enumerate(app_ids):
                if i % 100 == 0:
                    self.log.info(f"Processed {i} apps")
                
                url = f"{self.BASE_URL}{app_id}"
                response = self.get_request(url, max_attempts=3, params=self.params, headers=self.headers)
                if not response:
                    self.log.warning(f"No data returned for app_id: {app_id}")
                    continue

                try:
                    output_file.write(json.dumps(response.json(), ensure_ascii=False) + "\n")
                except Exception as e:
                    self.log.exception(f"Failed to get and write JSON for {app_name}: {e}")

                time.sleep(self.REQUEST_INTERVAL_TIME)

        self.log.info(f"Finished scraping all data for {len(app_ids)} apps (from index {start} to {end})")


if __name__ == "__main__":
    START = 0
    LIMIT = 25000
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_gamalytic.log", mode="w" if START == 0 else "a"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    steam_scraper = GamalyticScraper()
    steam_scraper.get_data(start=START, limit=LIMIT)
