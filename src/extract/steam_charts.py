import logging
from api_scraper import APIScraper


class SteamPlayerCharts(APIScraper):
    def __init__(self):
        super().__init__("https://steamcharts.com/app/")
        self.log = logging.getLogger(__name__)
        self.data_file = "../../data/raw/steam_charts/ccu_history.jsonl"
        self.id_file = "../../data/raw/steam_ids/app_ids.txt"
        self.end_path = "/chart-data.json"

    def get_ccu_history_id(self, id: int) -> str | None:
        """
        Fetch historical API data of given Steam ID app

        Args:
            id: Steam App ID
        Returns:
            str: JSON return object as string if exists
        """
        url = f"{self.BASE_URL}{id}{self.end_path}"
        response = self.get_request(url, 1, headers=self.headers, exit_on_fail=False)
        return response.text if response else None

    def get_all_ccu_history(self) -> None:
        """
        Records all historical data in `self.data_file` for each `id` in `self.id_file`
        """
        steam_id_file = open(self.id_file, mode="r")
        output_data = open(self.data_file, mode="w")
        for app in steam_id_file:
            id_data = app.split("\t")
            if len(id_data) == 1:
                continue

            app_id = int(id_data[0])
            ccu_data = self.get_ccu_history_id(app_id)
            if not ccu_data or len(ccu_data) == 0:
                self.log.warning(
                    f"Failed to find CCU history for id={app_id}, {id_data[1].strip()}"
                )
                continue

            output_data.write(f"{app_id}\t{ccu_data}")

        steam_id_file.close()


if __name__ == "__main__":
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steamcharts_ccu.log", mode="w"
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
    steamcharts_scraper.get_all_ccu_history()
