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
        self.data_file = output_dir + "game_data.jsonl"
        self.id_file = output_dir + "game_ids.txt"

    def get_search_key(self, max_attempts: int = 3) -> str | None:
        """
        Searches for search key used in /api/search/{key}.

        As of Oct 12, 2024, HLTB generates a {key} for the search api
        every ~24 hours, which can be obtained from a `<script>` object
        referenced at the base URL's HTML.

        This method will obtain the location of that script object in the
        base HTML, then search for the key in the JS text.

        Args:
            max_attempts (int): maximum number of fetch retries

        Returns:
            str: hexadecimal search key
        """
        self.log.info("Scraping search key...")

        attempt_count = 0
        while attempt_count < max_attempts:
            attempt_count += 1
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
                    exit(1)

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
                    exit(1)

            except HTTPError as e:
                if e in self.RETRY_CODES:
                    self.log.warning(f"    HTTPError in finding search UUID: {e}")
                else:
                    self.log.exception(f"    Fatal error in finding search UUID: {e}")
                    exit(1)

            time.sleep(self.RETRY_TIME)

        self.log.error(f"Failed to find search key in {max_attempts} attempts")
        exit(1)

    def __get_searchbody__(self) -> dict:
        """
        Get the request body for the search API.
        Modify `searchPage` instead of getting a new body.

        Returns:
            dict: JSON body
        """

        return {
            "searchType": "games",
            "searchTerms": [""],
            "searchPage": 1,  # can just modify this each time
            "size": 20,
            "searchOptions": {
                "games": {
                    "userId": 0,
                    "platform": "",
                    "sortCategory": "popular",
                    "rangeCategory": "main",
                    "rangeTime": {"min": None, "max": None},
                    "gameplay": {"perspective": "", "flow": "", "genre": ""},
                    "rangeYear": {"min": "", "max": ""},
                    "modifier": "",
                },
                "users": {"sortCategory": "postcount"},
                "lists": {"sortCategory": "follows"},
                "filter": "",
                "sort": 0,
                "randomizer": 0,
            },
            "useCache": True,
        }

    def __record_search_page__(self, data: list) -> None:
        """
        Records and appends data into `self.id_file`.

        Args:
            data (list): List of 20 game id ints
        """

        with open(self.id_file, "a") as f:
            for game_id in data:
                f.write(f"{game_id}\n")

    def get_hltb_ids_page(
        self,
        page: int,
        search_url: str,
        search_body: dict,
        max_attempts_per_page: int,
    ) -> tuple[list, bool]:
        """
        Obtains list of ids from a given page

        Args:
            page (int): search page
            search_url (str): search API url
            search_body (dict): request body dictionary
            max_attempts_per_page (int): maximum number of request retries

        Returns:
            list: list of HLTB ids (each `int`)
            bool: whether or not this is the last page
        """
        attempt_count = 0
        while attempt_count < max_attempts_per_page:
            try:
                search_body["searchPage"] = page
                response = requests.post(
                    search_url, json=search_body, headers=self.HEADERS
                )
                response.raise_for_status()

                results = response.json()
                if results["pageCurrent"] != page:
                    self.log.warning(
                        f"    Requested page number {page} does not match response page number {results["pageCurrent"]}"
                    )
                
                is_final_page = results["pageCurrent"] == results["pageTotal"]

                ids = []
                for game in results["data"]:
                    ids.append(game["game_id"])

                self.log.debug(f"    Found {len(ids)} results on page {page}")
                return ids, is_final_page

            except HTTPError as e:
                if e in self.RETRY_CODES:
                    self.log.warning(f"    HTTPError with page {page}: {e}")
                else:
                    self.log.exception(f"    Fatal error in searching page {page}: {e}")
                    exit(1)
            time.sleep(self.RETRY_TIME)

        self.log.error(
            f"Failed to find data from page {page} in {max_attempts_per_page} attempts"
        )
        exit(1)

    def get_hltb_ids(
        self,
        search_key: str,
        max_attempts_per_page: int = 3,
        reset_id_file: bool = True,
    ) -> None:
        """ """
        # empty out the output id file
        if reset_id_file:
            self.log.warning(f"Emptying or creating {self.id_file}")
            with open(self.id_file, "w") as f:
                f.write("")

        search_body = self.__get_searchbody__()
        search_url = f"{self.BASE_URL}/api/search/{search_key}"

        # iterate through all pages on HLTB
        page = 1
        while True:
            self.log.info(f"Requesting search page {page}")
            game_ids_from_page, is_final_page = self.get_hltb_ids_page(
                page, search_url, search_body, max_attempts_per_page
            )

            self.__record_search_page__(game_ids_from_page)
            
            if is_final_page:
                break

            time.sleep(self.REQUEST_INTERVAL_TIME)
            page += 1

        return


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
    key = hltb_scraper.get_search_key()
    hltb_scraper.get_hltb_ids(key)
