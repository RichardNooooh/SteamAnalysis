import logging
import time
from api_scraper import APIScraper
import json

class AppDetailsScraper(APIScraper):
    def __init__(self):
        super.__init__("http://store.steampowered.com/api/appdetails")
        self.id_file = "../../data/raw/steam_ids/game_ids.txt"
        self.data_file = "../../data/raw/steam/appdetails.jsonl"
        self.REQUEST_INTERVAL_TIME = 0.25

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
        
        response = self.get_request(url, max_attempts=3, params=query, exit_on_fail=False)
        if response is None:
            self.log.warning(f"Failed to retrieve appdetails data for {app_id}")
            return None
    
    def get_appdetails(self, start=0, limit=None):
        """
        """
        self.log.info(f"Reading app IDs from {self.id_file}")
        pass

