from fake_useragent import UserAgent
from http import HTTPStatus

class APIScraper:
    def __init__(self, url):
        self.BASE_URL = url

        ua = UserAgent()
        self.HEADERS = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": self.BASE_URL,
            "Referer": self.BASE_URL,
            "User-Agent": ua.chrome,
        }

        self.RETRY_CODES = [
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.GATEWAY_TIMEOUT,
            HTTPStatus.SERVICE_UNAVAILABLE
        ]
        
        self.RETRY_TIME = 5.0    # seconds
        self.REQUEST_INTERVAL_TIME = 0.5 # seconds

    def run_scraper(self):
        raise NotImplementedError()
