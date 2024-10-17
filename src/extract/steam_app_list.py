import logging
import time
from api_scraper import APIScraper
import json

class SteamAppList(APIScraper):
    """
    Retrieves all game and DLC app IDs on Steam, not including ones still in Early Access
    or exclusively available outside of the US. This list will be used for future interactions with
    the app detail retrievals from the Steam API and Gamalytic.

    This is done in two steps:
    1. Retrieve the raw list of Steam Apps from `https://api.steampowered.com/ISteamApps/GetAppList/v2/`
        - This data includes Games/DLCs, but also contains Demos, "Videos", Soundtracks, and more.
        - For whatever reason, sometimes returns duplicates...
    2. Filter out nonvisible IDs and non-game related apps with `https://api.steampowered.com/IStoreBrowseService/GetItems/v1`
    """
    def __init__(self):
        super().__init__("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
        self.raw_id_file = "../../data/raw/steam_ids/_ids_raw.txt"

        self.item_URL = "https://api.steampowered.com/IStoreBrowseService/GetItems/v1"
        self.game_id_file = "../../data/raw/steam_ids/game_ids.txt"
        self.dlc_id_file =  "../../data/raw/steam_ids/dlc_ids.txt"

        self.query = {"key": self.STEAM_API_KEY}
        self.filter_query = {
            "ids": None,
            "context": {
                "language": "english",
                "country_code": "US",
                "steam_realm": 1,
            }
        }

        self.REQUEST_INTERVAL_TIME = 1.0


    def get_app_list(self) -> None:
        """
        Fetches list of all Steam app IDs with GET request to API. Writes to `self.raw_id_file`
        Uses API key since it may or may not affect the output.
        """
        self.log.info("Beginning Steam app ID data retrieval")
        response = self.get_request(
            self.BASE_URL, 1, params=self.query, headers=self.headers
        )

        app_data = response.json()["applist"]["apps"]
        app_data = [(app["appid"], app["name"]) for app in app_data]

        self.log.info(f"Successfully retrieved {len(app_data)} apps")
        if len(app_data) != len(set(app_data)):
            self.log.warning(
                f"Found {len(app_data) - len(set(app_data))} duplicates in the list"
            )

        self.log.info(f"Writing data into {self.raw_id_file}")
        with open(self.raw_id_file, mode="w") as f:
            for app in app_data:
                f.write(f"{app[0]}\t{app[1]}\n")
        self.log.info("Successfully finished writing all raw app ids")


    def process_batch(self, store_items: dict, names_batch: dict, game_ids: list, dlc_ids: list) -> None:
        """
        Using the data from `store_items`, append IDs (and corresponding names from `names_batch`)
        to `game_ids` and `dlc_ids`.

        Keep in mind the API can change at any point.
        """
        for item in store_items:
            name = names_batch[item["appid"]]
            if not item["visible"]:
                self.log.warning(f"Item ID {item["appid"]} (name={name}) is not visible")
                continue
            
            if "type" not in item:
                self.log.warning(f"Visible app {item["appid"]} (name={name}) does not have `type` field")
                continue
            
            if item["type"] == 0: # Games
                game_ids.append((item["appid"], name))
            elif item["type"] == 4: # DLC
                dlc_ids.append((item["appid"], name))
            else:
                self.log.debug(f"Filtered out {item["appid"]} (name={name})")


    def filter_app_list(self, batch_size: int = 100) -> None:
        """
        Using IDs from `self.raw_id_file`, filter games (type=0) and DLC (type=4) items and record
        them into `self.game_id_file` and `self.dlc_id_file`.

        This is obtained with a GET request of multiple app IDs to:
        `https://api.steampowered.com/IStoreBrowseService/GetItems/v1`.

        This API endpoint requires a single query parameter, `input_json`.

        Args:
            batch_size (int): number of app ids per request
        """
        self.log.info("Beginning filtering process")
        app_ids = []
        with open(self.raw_id_file, "r") as f:
            for app in f:
                data = app.split("\t")
                app_ids.append((int(data[0]), data[1].strip()))
        self.log.info(f"Finished reading {len(app_ids)} app IDs from {self.raw_id_file}")

        total_apps = len(app_ids)
        game_ids, dlc_ids = [], []

        for i in range(0, total_apps, batch_size):
            if i % 1000 == 0:
                self.log.info(f"Processed {i} apps")

            batch = app_ids[i : i + batch_size]
            self.filter_query["ids"] = [{"appid": app[0]} for app in batch]
            names_batch = {app[0]: (app[1] if len(app) == 2 else "") for app in batch}

            response = self.get_request(self.item_URL, 3, params={"input_json": json.dumps(self.filter_query)})
            self.log.debug(f"Successfully retrieved data from {response.url}")
            store_items = response.json().get("response", {}).get("store_items", [])

            if len(store_items) != len(batch):
                self.log.warning(
                    f"Number of apps retrieved ({len(store_items)}) does "
                    + f"not match number requested ({len(batch)}) "
                    + f"in range {i}, {i+batch_size}"
                )

            self.process_batch(store_items, names_batch, game_ids, dlc_ids)
            time.sleep(self.REQUEST_INTERVAL_TIME)
        
        self.log.info(f"Processed all apps: #Games={len(game_ids)}, #DLC={len(dlc_ids)}")
        with open(self.game_id_file, mode="w") as f:
            for app in game_ids:
                f.write(f"{app[0]}\t{app[1]}\n")

        with open(self.dlc_id_file, mode="w") as f:
            for app in dlc_ids:
                f.write(f"{app[0]}\t{app[1]}\n")


if __name__ == "__main__":
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler("../../logs/extract_steam_app_ids.log", mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    steamapp_scraper = SteamAppList()
    # steamapp_scraper.get_app_list()
    steamapp_scraper.filter_app_list()

