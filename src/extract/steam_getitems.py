import logging
import time
from api_scraper import APIScraper
import json


class SteamGetItemsScraper(APIScraper):
    def __init__(self):
        super().__init__("https://api.steampowered.com/IStoreBrowseService/GetItems/v1")
        self.id_folder = "../../data/raw/steam_ids/"
        self.id_files = ["game_ids.txt", "dlc_ids.txt"]
        self.data_file = "../../data/raw/steam_apps/getitems"

        self.REQUEST_INTERVAL_TIME = 2.0

        self.filter_query = {
            "ids": None,
            "context": {
                "language": "english",
                "country_code": "US",
                "steam_realm": "1",
            },
            "data_request": {
                "include_release": True,
                "include_platforms": True,
                "include_all_purchase_options": True,
                "include_ratings": True,
                "include_tag_count": "20",
                "include_basic_info": True,
                "include_supported_languages": True,
            },
        }

        self.log = logging.getLogger(__name__)


    def process_batch(self, file_handle, store_items: list) -> None:
        """
        Writes each item's JSON data to a file, one item per line.

        Args:
            file_handle (file object): The open file to write the data to.
            store_items (list): List of items (apps) to write.
        """
        for item in store_items:
            file_handle.write(json.dumps(item, ensure_ascii=False) + "\n")


    def get_getitems(
        self, start: int = 0, limit: int = 10000, batch_size: int = 50
    ) -> None:
        """
        Iterates through the app IDs, processes them in batches, submits a request to the Steam API, 
        and stores the JSON responses.

        Args:
            start (int): The starting index in the id_file to begin scraping from.
            limit (int): The number of app_ids to scrape.
            batch_size (int): The number of app_ids to send in each batch request.
        """
        # Read in data
        self.log.info(f"Reading app IDs from {self.id_folder}{self.id_files}")
        app_ids_names = []
        for file_name in self.id_files:
            with open(f"{self.id_folder}{file_name}", mode="r") as f:
                for app in f:
                    data = app.split("\t")
                    app_ids_names.append((int(data[0]), data[1].strip()))
        self.log.info(f"Read {len(app_ids_names)} IDs from {self.id_folder}")
        app_ids_names = app_ids_names[start : start + limit]
        total_apps = len(app_ids_names)

        end = start + total_apps - 1
        self.log.info(f"Starting API scraping for {total_apps} apps (from index {start} to {end})")

        # Begin app data retrieval
        file_name = f"{self.data_file}_{start}_{end}.jsonl"
        self.log.info(f"Saving data to {file_name}")
        with open(file_name, mode="w") as output_file:
            for i in range(0, total_apps, batch_size):
                batch = app_ids_names[i : i + batch_size]
                self.filter_query["ids"] = [{"appid": app[0]} for app in batch]

                response = self.get_request(
                    self.BASE_URL, 3, params={"input_json": json.dumps(self.filter_query)}
                )
                self.log.debug(f"Successfully retrieved data from {response.url}")
                store_items = response.json().get("response", {}).get("store_items", [])

                if len(store_items) != len(batch):
                    self.log.warning(
                        f"Number of apps retrieved ({len(store_items)}) does "
                        + f"not match number requested ({len(batch)}) "
                        + f"in range {i}, {i+batch_size}"
                    )

                self.process_batch(output_file, store_items)
                self.log.info(f"Processed batch {i // batch_size + 1}: {len(batch)} app IDs")

                time.sleep(self.REQUEST_INTERVAL_TIME)
            
        self.log.info(f"Finished scraping app details for {len(app_ids_names)} apps (from index {start} to {end})")


if __name__ == "__main__":
    START = 20000
    LIMIT = 20000
    BATCH_SIZE = 50
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steam_getitems.log", mode="w" if START == 0 else "a"
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

    scraper = SteamGetItemsScraper()
    scraper.get_getitems(start=START, limit=LIMIT, batch_size=BATCH_SIZE)
