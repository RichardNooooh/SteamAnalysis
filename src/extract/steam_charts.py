import logging
from api_scraper import APIScraper
import time
from bs4 import BeautifulSoup

class SteamPlayerCharts(APIScraper):
    """
    This extracts concurrent player count data from `steamcharts.com`.

    It's unfortunately kind of unreliable to get data from all available steam apps without
    overwhelming the API with requests. This website stores data for ~12400 games while there
    are ~110000 games on steam.

    Fortunately, the data for all games that are even remotely decent should be of decent quality.
    This will be used to assess the quality of player count data from Gamalytic.
    """
    def __init__(self):
        super().__init__("https://steamcharts.com/")
        self.end_path = "chart-data.json"

        self.log = logging.getLogger(__name__)
        self.id_file = "../../data/raw/steam_charts/chart_ids.txt"
        self.data_file = "../../data/raw/steam_charts/ccu_history"

        self.REQUEST_INTERVAL_TIME = 1.0  # seconds


    def get_charted_steam_ids_from_page(self, pagenum: int) -> list:
        """
        Fetches the steam app IDs from given `steamcharts.com` top games page.

        Note: It may skip or repeat certain IDs because the `top` page is determined by current player counts.

        Args:
            page_num (int): Page number requested
        Returns:
            list: List of steam IDs found on page
        """
        url = f"{self.BASE_URL}top/p.{pagenum}"
        response = self.get_request(url, max_attempts=3, headers=self.headers, exit_on_fail=False)
        if not response:
            self.log.warning(f"Failed to retrieve data. We are likely at the last page on page #{pagenum}.")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find("tbody")

        if not table:
            self.log.warning(f"No table-body found on page {pagenum}. Stopping scrape process")
            return []
        
        steam_ids = []
        for row in table.find_all("tr"):
            game_name_element = row.find("td", {"class": "game-name left"})
            if game_name_element:
                link_tag = game_name_element.find("a")
                if link_tag:
                    steam_id = link_tag['href'].split("/")[2]
                    steam_ids.append(steam_id)
                else:
                    self.log.warning("Missing <a> tag")
            else:
                self.log.warning("Missing <td> tag with class \"game-name left\"")
        
        self.log.info(f"Found {len(steam_ids)} IDs on page {pagenum}")
        return steam_ids


    def get_all_charted_ids(self):
        """
        Iterates through `steamcharts.com/top/` pages and extracts steam IDs
        Stores results into `self.id_file`.
        """
        steam_ids = []
        page_num = 1
        self.log.info("Beginning charted steam ID requests")
        while True:
            self.log.info(f"Requesting page {page_num}")
            ids_from_page = self.get_charted_steam_ids_from_page(page_num)
            
            if len(ids_from_page) == 0:
                break
            
            steam_ids.extend(ids_from_page)
            page_num += 1

            time.sleep(self.REQUEST_INTERVAL_TIME * 2)
        
        self.log.info(f"Scraping finished. Writing {len(steam_ids)} Steam IDs to file.")
        with open(self.id_file, mode="w") as f:
            for steam_id in steam_ids:
                f.write(f"{steam_id}\n")
        
        self.log.info(f"Successfully saved Steam IDs to {self.id_file}")


    def get_ccu_history_id(self, id: int) -> list | None:
        """
        Fetch historical API data of given Steam ID app

        Args:
            id: Steam App ID
        Returns:
            list: JSON return object as list if exists
        """
        url = f"{self.BASE_URL}app/{id}/{self.end_path}"
        response = self.get_request(url, 1, headers=self.headers, exit_on_fail=False)
        return response.json() if response else None


    def get_all_ccu_history(self, start: int = 0, limit: int = 25000) -> None:
        """
        Records all historical data in `self.data_file` for each `id` in `self.id_file`
        """
        self.log.info(f"Reading from {self.id_file}")
        app_ids = []
        with open(self.id_file, mode="r") as f:
            for app in f:
                app_ids.append(int(app.strip()))
        app_ids = app_ids[start : start + limit]

        end = start + len(app_ids) - 1
        output_file_name = f"{self.data_file}_{start}_{end}.jsonl"

        self.log.info(
            "Beginning retrieval of /appdetail/ "\
            + f"data for {len(app_ids)} games, from index {start} "\
            + f"to {end} (inclusive) into {output_file_name}"
        )

        # if start is 0, then begin writing from the beginning
        file_mode = "w" if start == 0 else "a"
        with open(output_file_name, mode=file_mode) as output_data:            
            i, count = 0, 0
            for app_id in app_ids:
                i += 1
                if i % 100 == 0:
                    self.log.info(f"Retrieved status / data for {i} games")

                ccu_data = self.get_ccu_history_id(app_id)
                if not ccu_data or len(ccu_data) == 0:
                    self.log.warning(
                        f"Failed to find CCU history for id={app_id}"
                    )
                    continue
                count += 1
                output_data.write(f"{app_id}\t{ccu_data}\n")
                time.sleep(self.REQUEST_INTERVAL_TIME)

        self.log.info(f"Finished recording the CCU history for {count} games.")


if __name__ == "__main__":
    START = 0
    LIMIT = 5000
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steamcharts_ccu.log", mode="w" if START == 0 else "a"
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

    steamcharts_scraper = SteamPlayerCharts()
    # steamcharts_scraper.get_all_charted_ids()
    steamcharts_scraper.get_all_ccu_history(start=START, limit=LIMIT)
