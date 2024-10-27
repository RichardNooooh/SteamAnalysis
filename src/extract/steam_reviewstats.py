import logging
import time
from api_scraper import APIScraper
import json
from copy import deepcopy
import os
from datetime import datetime, timedelta

class SteamReviewStatisticsScraper(APIScraper):
    def __init__(self):
        super().__init__("https://store.steampowered.com/appreviews/")
        self.id_file = "../../data/raw/steam_ids/game_ids.txt"
        self.data_all_file = "../../data/raw/steam_apps/review_summary_all"
        self.data_early_file = "../../data/raw/steam_apps/review_summary_early" # first two weeks after release
        self.REQUEST_INTERVAL_TIME = 0.5

        self.getitems_directory = "../../data/raw/steam_apps/"

        self.params = {
            "json": 1,
            "use_review_quality": True,
            "cursor": "*",
            "date_range_type": "include",
            "language": "all",
            "l": "all",
            "review_type": "all",
            "purchase_type": "steam",
            "start_date": -1,
            "end_date": -1
        }


    def submit_and_write_request(self, url, query_parameters, app_id, app_name, output_file):
        response = self.get_request(url, max_attempts=3, params=query_parameters, exit_on_fail=False)
        if not response:
            self.log.warning(f"No data returned for app_id: {app_id}")
            # continue

        try:
            # I don't care about storing the review text data with this. 
            # Should be doing this in the "transform" stage, but this is a lot of data
            json_data = response.json()
            json_data["id"] = app_id
            json_data.pop("reviews", None) 
            json_data.pop("success", None)

            output_file.write(json.dumps(json_data, ensure_ascii=False) + "\n")
        except Exception as e:
            self.log.exception(f"Failed to get and write JSON for {app_name}: {e}")


    def get_releasedates(self):
        self.log.info("Reading release date data from stored getitems/ endpoint")
        id_release_dict = {}

        for filename in os.listdir(self.getitems_directory):
            if filename.startswith("getitems_"):
                file_path = os.path.join(self.getitems_directory, filename)
                
                with open(file_path, mode='r') as f:
                    for line in f:
                        data = json.loads(line.strip())

                        app_id = data["id"]
                        if "release" not in data:
                            self.log.warning(f"No release date for {app_id}")
                            continue

                        release_date_struct = data["release"]

                        if "is_coming_soon" in release_date_struct and release_date_struct["is_coming_soon"] is True:
                            continue
                        if "is_early_access" in release_date_struct and release_date_struct["is_early_access"] is True:
                            continue
                        if "steam_release_date" in release_date_struct:
                            id_release_dict[app_id] = release_date_struct["steam_release_date"]
                        else:
                            self.log.warning(f"No steam release date for {app_id}")
        
        return id_release_dict


    def get_timestamp_end(self, timestamp_start: int, weeks: int = 2):
        datetime_start = datetime.fromtimestamp(timestamp_start)
        datetime_end = datetime_start + timedelta(weeks=weeks)
        return int(datetime_end.timestamp())


    def get_data(self, start: int = 0, limit: int = 10000):
        """
        Fetches all data for a range of IDs.
        Args:
            start (int): Starting index of the IDs.
            limit (int): Number of IDs to process.
        """
        self.log.info("Beginning data retrieval from Steam for Review Summaries")
        self.log.info(f"Reading app IDs from {self.id_file}")
        app_ids = []
        with open(self.id_file, mode="r") as f:
            for app in f:
                data = app.split("\t")
                if len(data) == 1:
                    continue
                app_ids.append((int(data[0]), data[1].strip()))

        app_ids = app_ids[start:start + limit]
        end = start + len(app_ids) - 1

        self.log.info(f"Starting API scraping for {len(app_ids)} apps (from index {start} to {end})")

        release_dates = self.get_releasedates()

        file_all_name = f"{self.data_all_file}_{start}_{end}.jsonl"
        self.log.info(f"Saving data to {file_all_name} for all-time data")

        file_early_name = f"{self.data_early_file}_{start}_{end}.jsonl"
        self.log.info(f"Saving data to {file_early_name} for data two weeks after release")
        with open(file_all_name, mode="w") as output_all_file, \
             open(file_early_name, mode="w") as output_early_file:
            for i, (app_id, app_name) in enumerate(app_ids):
                if i % 100 == 0:
                    self.log.info(f"Processed {i} apps")

                url = f"{self.BASE_URL}{app_id}"

                # get data for all time
                query_parameters = deepcopy(self.params)
                self.submit_and_write_request(url, query_parameters, app_id, app_name, output_all_file)
                time.sleep(self.REQUEST_INTERVAL_TIME)

                if app_id not in release_dates:
                    continue

                # get data from two weeks after release
                time_start = release_dates[app_id]
                time_end = self.get_timestamp_end(time_start)

                query_parameters["start_date"] = time_start
                query_parameters["end_date"] = time_end
                self.submit_and_write_request(url, query_parameters, app_id, app_name, output_early_file)
                time.sleep(self.REQUEST_INTERVAL_TIME)


        self.log.info(f"Finished scraping all data for {len(app_ids)} apps (from index {start} to {end})")


if __name__ == "__main__":
    START = 0
    LIMIT = 20000
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steamreview_stats.log", mode="w" if START == 0 else "a"
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

    steam_scraper = SteamReviewStatisticsScraper()
    steam_scraper.get_data(start=START, limit=LIMIT)
