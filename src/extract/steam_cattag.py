import logging
from api_scraper import APIScraper
import json


class SteamCategoriesTags(APIScraper):
    def __init__(self):
        super().__init__("")
        self.tag_url = "https://store.steampowered.com/actions/ajaxgetstoretags"
        self.category_url = (
            "https://api.steampowered.com/IStoreBrowseService/GetStoreCategories/v1"
        )

        self.tag_file = "../../data/raw/steam_ids/tags.json"
        self.category_file = "../../data/raw/steam_ids/categories.json"

        self.category_query = {"language": "english"}
        self.log = logging.getLogger(__name__)

    def get_tags(self):
        """
        Fetches tags from the Steam API and save into `self.tag_file`.
        """
        self.log.info(f"Fetching tags from {self.tag_url}")
        response = self.get_request(self.tag_url, max_attempts=3)
        tags_data = response.json()

        with open(self.tag_file, "w") as f:
            json.dump(tags_data, f, indent=4)

        self.log.info(f"Successfully saved tags to {self.tag_file}")

    def get_categories(self):
        """
        Fetch categories from Steam Store and save into `self.category_file`
        """
        self.log.info(f"Fetching categories from {self.category_url}")
        response = self.get_request(
            self.category_url, max_attempts=3, params=self.category_query
        )
        categories_data = response.json()

        with open(self.category_file, "w") as f:
            json.dump(categories_data, f, indent=4)

        self.log.info(f"Successfully saved categories to {self.category_file}")


if __name__ == "__main__":
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(
        "../../logs/extract_steam_categories_tags.log", mode="w"
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

    steam_scraper = SteamCategoriesTags()
    steam_scraper.get_tags()
    steam_scraper.get_categories()
