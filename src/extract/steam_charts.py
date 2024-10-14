import logging
from api_scraper import APIScraper
import time


class SteamPlayerCharts(APIScraper):
    def __init__(self):
        super().__init__("https://steamcharts.com/app/")
        self.log = logging.getLogger(__name__)
        self.data_file = "../../data/raw/steam_charts/ccu_history.jsonl"
        self.id_file = "../../data/raw/steam_ids/game_ids.txt"
        self.end_path = "/chart-data.json"
        self.REQUEST_INTERVAL_TIME = 0.75  # seconds

        self.charted_id_file = "../../data/raw/steam_charts/game_ids_charted.txt"
        self.uncharted_id_file = "../../data/raw/steam_charts/game_ids_uncharted.txt"

    def get_ccu_history_id(self, id: int) -> list | None:
        """
        Fetch historical API data of given Steam ID app

        Args:
            id: Steam App ID
        Returns:
            list: JSON return object as list if exists
        """
        url = f"{self.BASE_URL}{id}{self.end_path}"
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
                data = app.split("\t")
                if len(data) == 1:
                    continue
                app_ids.append((int(data[0]), data[1].strip()))
        app_ids = app_ids[start : start + limit]

        # if start is 0, then begin writing from the beginning
        file_mode = "w" if start == 0 else "a"

        self.log.info(
            "Beginning retrieval of concurrent player count "\
            + f"histories for {len(app_ids)} games, from index {start} "\
            + f"to {start+limit-1} (inclusive)"
        )

        with open(self.data_file, mode=file_mode) as output_data, \
             open(self.charted_id_file, mode=file_mode) as charted_file, \
             open(self.uncharted_id_file, mode=file_mode) as uncharted_file:
            
            i, count = 0, 0
            for app_id, name in app_ids:
                i += 1
                if i % 100 == 0:
                    self.log.info(f"Retrieved status / data for {i} games")

                ccu_data = self.get_ccu_history_id(app_id)
                if not ccu_data or len(ccu_data) == 0:
                    self.log.warning(
                        f"Failed to find CCU history for id={app_id}, {name}"
                    )
                    uncharted_file.write(f"{app_id}: {name}\n")
                    continue
                count += 1
                output_data.write(f"{app_id}\t{ccu_data}\n")
                charted_file.write(f"{app_id}: {name}\n")
                time.sleep(self.REQUEST_INTERVAL_TIME)

        self.log.info(f"Finished recording the CCU history for {count} games.")


if __name__ == "__main__":
    START = 0
    LIMIT = 10000
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
    steamcharts_scraper.get_all_ccu_history(start=START, limit=LIMIT)
