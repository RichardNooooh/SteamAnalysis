import logging
from api_scraper import APIScraper

class SteamAppList(APIScraper):
    def __init__(self):
        super().__init__("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
        self.log = logging.getLogger(__name__)
        self.data_file = "../../data/raw/steam_ids/app_ids.txt"
        self.query = {"key": self.STEAM_API_KEY}
    
    def get_app_list(self) -> None:
        """
        Fetches list of all Steam app IDs with GET request to API. Writes to `self.data_file`
        Uses API key since it may or may not affect the output... (hard to tell)
        """
        response = self.get_request(self.BASE_URL, 1, params=self.query, headers=self.headers)
        
        app_data = response.json()["applist"]["apps"]
        app_data = [(app["appid"], app["name"]) for app in app_data]
        
        self.log.info(f"Successfully retrieved {len(app_data)} apps")
        if len(app_data) != len(set(app_data)):
            self.log.warning(f"Found {len(app_data) - len(set(app_data))} duplicates in the list")

        self.log.info(f"Writing data into {self.data_file}")
        with open(self.data_file, mode='w') as f:
            for app in app_data:
                f.write(f"{app[0]}\t{app[1]}\n")
        self.log.info("Successfully finished writing data")

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
    steamapp_scraper.get_app_list()
