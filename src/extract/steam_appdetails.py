import logging
import time
from api_scraper import APIScraper
import json

class SteamAppDetailsScraper(APIScraper):
    def __init__(self):
        super().__init__("http://store.steampowered.com/api/appdetails")
        self.id_folder = "../../data/raw/steam_ids/"
        self.id_files = ["game_ids.txt", "dlc_ids.txt"]
        self.data_file = "../../data/raw/steam_apps/appdetails"
        self.REQUEST_INTERVAL_TIME = 1.5 # rate limited by 200 req / 5 min (max 100k per day)

        self.log = logging.getLogger(__name__)


    def fetch_app(self, app_id: int) -> dict | None:
        """
        Fetches the JSON from the appdetails API. Note that
        the API does not accept multiple app_ids anymore.
        """
        query = {
            "cc": "US",
            "key": self.STEAM_API_KEY,
            "appids": app_id
        }

        response = self.get_request(self.BASE_URL, max_attempts=3, params=query, exit_on_fail=False)
        if response is None:
            self.log.warning(f"Failed to retrieve appdetails data for {app_id}")
            return None
        
        result = None
        try:
            result = response.json()
        except Exception as e:
            self.log.exception(f"Weird JSON with response for ID {app_id}. Response text: {response.text} Skipping...: {e}")

        return result

    
    def get_appdetails(self, start=0, limit=10000) -> None:
        """
        Iterates through the filtered apps list and slowly retrieves appdetail data
        for each app.
        """
        self.log.info(f"Reading app IDs from {self.id_folder}{self.id_files}")
        app_ids_names = []
        for file_name in self.id_files:
            with open(f"{self.id_folder}{file_name}", mode='r') as f:
                for app in f:
                    data = app.split("\t")
                    app_ids_names.append((int(data[0]), data[1].strip()))
        self.log.info(f"Read {len(app_ids_names)} IDs from {self.id_folder}")
        app_ids_names = app_ids_names[start : start + limit]

        end = start + len(app_ids_names) - 1
        output_file_name = f"{self.data_file}_{start}_{end}.jsonl"

        self.log.info(
            "Beginning retrieval of /appdetail/ "\
            + f"data for {len(app_ids_names)} games, from index {start} "\
            + f"to {end} (inclusive) into {output_file_name}"
        )
        # file_mode = "w" if start == 0 else "a"

        with open(output_file_name, mode="w") as output_file:
            for i, (app_id, name) in enumerate(app_ids_names):
                if i % 100 == 0:
                    self.log.info(f"Processed {i} app IDs")

                app_details = self.fetch_app(app_id)
                
                if not app_details:
                    self.log.warning(f"No data returned for app_id: {app_id} (name={name})")
                    continue
                
                output_file.write(json.dumps(app_details, ensure_ascii=False) + "\n")
                time.sleep(self.REQUEST_INTERVAL_TIME)

        self.log.info(f"Finished scraping app details for {len(app_ids_names)} apps")



if __name__ == "__main__":
    START = 80000
    LIMIT = 20000
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steam_appdetails.log", mode="w" if START == 0 else "a"
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

    steam_scraper = SteamAppDetailsScraper()
    steam_scraper.get_appdetails(start=START, limit=LIMIT)
