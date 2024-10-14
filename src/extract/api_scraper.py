from fake_useragent import UserAgent
from http import HTTPStatus
from dotenv import load_dotenv
import requests
from requests.exceptions import HTTPError
from requests import Response
import os
import time


class APIScraper:
    def __init__(self, url):
        self.BASE_URL = url

        ua = UserAgent()
        self.headers = {
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
            HTTPStatus.SERVICE_UNAVAILABLE,
        ]

        self.RETRY_TIME = 5.0  # seconds
        self.REQUEST_INTERVAL_TIME = 0.5  # seconds

        load_dotenv("../../.env")
        self.STEAM_API_KEY = os.getenv("STEAM_API_KEY")
        if not self.STEAM_API_KEY:
            raise ValueError("Steam API key not found")

    def get_request(
        self,
        url: str,
        max_attempts: int,
        params: dict = None,
        headers: dict = None,
        exit_on_fail: bool = True,
    ) -> Response | None:
        attempt_count = 0
        while attempt_count < max_attempts:
            attempt_count += 1
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                return response
            except HTTPError as e:
                if e in self.RETRY_CODES and attempt_count < max_attempts:
                    self.log.warning("Received server-side HTTPError. Retrying...")
                else:
                    self.log.warning("Received HTTPError. Stopping request...")
                    break
            time.sleep(self.RETRY_TIME)

        if exit_on_fail:
            self.log.error(
                f"Failed to retrieve successful response from {url} "
                + f"within {max_attempts} attempts"
            )
            self.log.error(f"    params={params}")
            self.log.error(f"    headers={headers}")
            exit(1)

        return None

    def post_request(
        self,
        url: str,
        max_attempts: int,
        body: dict,
        params: dict = None,
        headers: dict = None,
        exit_on_fail: bool = False,
    ) -> Response | None:
        attempt_count = 0
        while attempt_count < max_attempts:
            attempt_count += 1
            try:
                response = requests.get(url, json=body, params=params, headers=headers)
                response.raise_for_status()
                return response
            except HTTPError as e:
                if e in self.RETRY_CODES and attempt_count < max_attempts:
                    self.log.warning("Received server-side HTTPError. Retrying...")
                else:
                    self.log.warning("Received HTTPError. Stopping request...")
                    break
            time.sleep(self.RETRY_TIME)
            time.sleep(self.RETRY_TIME)

        if exit_on_fail:
            self.log.error(
                f"Failed to retrieve successful response from {url} "
                + f"within {max_attempts} attempts"
            )
            self.log.error(f"    params={params}")
            self.log.error(f"    headers={headers}")
            exit(1)

        return None

    def run_scraper(self):
        raise NotImplementedError()
