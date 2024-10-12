import re
import json
import time
import logging

import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup

from api_scraper import APIScraper


class HLTBScraper(APIScraper):
    def __init__(self, output_dir="../../data/raw/hltb/"):
        super().__init__("https://howlongtobeat.com")
        self.log = logging.getLogger(__name__)
        self.data_file = output_dir + "gamedata.jsonl"
        self.id_file = output_dir + "game_ids.txt"

    def get_search_key(self, max_attempts: int = 3) -> str | None:
        self.log.info("Beginning extraction of search key")

        attempt_count = 0
        while attempt_count < max_attempts:
            attempt_count += 1
            self.log.info(f"Attempt #{attempt_count} for search key")
            try:
                # Look for <script> with search key
                response = requests.get(self.BASE_URL, headers=self.HEADERS)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                script_key_tag = soup.find(
                    "script", src=re.compile(r"_next/static/chunks/pages/_app-.*\.js")
                )
                if not script_key_tag:
                    self.log.error(
                        "    Fatal error in finding <script> tag with src from _next/static/chunks/pages/_app-*.js"
                    )
                    return None

                # Obtain script JS data
                script_url = self.BASE_URL + script_key_tag["src"]
                response = requests.get(script_url, headers=self.HEADERS)
                response.raise_for_status()

                # regex search for: "/api/search/".concat("{KEY}")
                matched_regex = re.search(
                    r'"\/api\/search\/"\.concat\("([a-zA-Z0-9]+)"\)', response.text
                )

                if matched_regex:
                    search_key = matched_regex.group(1)
                    self.log.info(f"    Found search key: {search_key}")
                    return search_key
                else:
                    self.log.error("    Regex failed to find search key")
                    return None

            except HTTPError as e:
                if e in self.RETRY_CODES:
                    self.log.warning(f"    HTTPError in finding search UUID: {e}")
                else:
                    self.log.exception(f"    Fatal error in finding search UUID: {e}")
                    return None

            time.sleep(self.RETRY_TIME)

    
    def get_hltb_ids(self, search_key: str) -> list[str]:
        pass


if __name__ == "__main__":
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler("../../logs/scraper.log", mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    hltb_scraper = HLTBScraper()
    hltb_scraper.get_search_key()
