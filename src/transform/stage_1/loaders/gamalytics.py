import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.gamalytics import (
    GamalyticsMain, GamalyticsHistory, GamalyticsAudienceOverlap, 
    GamalyticsPlaytimeData, GamalyticsEstimateDetails, GamalyticsDLC, GamalyticsAttributes
)
import logging
from tqdm import tqdm
from .baseloader import BaseLoader

class GamalyticsDataLoader(BaseLoader):
    def __init__(self, data_folder):
        super().__init__(data_folder)
        self.logger.info("Initializing the GamalyticsDataLoader")


    def insert_data(self, model_class: Base, data: dict) -> None:
        try:
            record = model_class(**data)
            self.session.add(record)
            self.logger.debug(f"Successfully inserted data into {model_class.__tablename__}")
        except Exception as e:
            self.logger.exception(f"Failed to insert data for {model_class.__tableanme__}!\n{e}")


    def insert_jsonlist_data(self, model_class: Base, data: dict, key: str, additional_data: dict = None) -> None:
        for entry in data.get(key, []):
            entry_data = {**entry, **(additional_data or {})}
            self.insert_data(model_class, entry_data)


    def insert_attribute_data(self, data: dict) -> None:
        for attribute_type in ["tags", "genres", "features", "languages"]:
            for val in data.get(attribute_type, []):
                attribute_data = {
                    "steamId": data["steamId"],
                    "attributeType": attr_type[:-1],
                    "value": value
                }
                self.insert_data(GamalyticsAttributes, attribute_data)


    def insert_main_data(self, data):
        main_data = {
            "steamId": data['steamId'],
            "name": data.get('name'),
            "description": data.get('description'),
            "price": data.get('price'),
            "reviews": data.get('reviews'),
            "reviewsSteam": data.get('reviewsSteam'),
            "followers": data.get('followers'),
            "avgPlaytime": data.get('avgPlaytime'),
            "reviewScore": data.get('reviewScore'),
            "releaseDate": data.get('releaseDate'),
            "EAReleaseDate": data.get('EAReleaseDate'),
            "firstReleaseDate": data.get('firstReleaseDate'),
            "earlyAccessExitDate": data.get('earlyAccessExitDate'),
            "unreleased": data.get('unreleased'),
            "earlyAccess": data.get('earlyAccess'),
            "copiesSold": data.get('copiesSold'),
            "revenue": data.get('revenue'),
            "totalRevenue": data.get('totalRevenue'),
            "players": data.get('players'),
            "owners": data.get('owners'),
            "steamPercent": data.get('steamPercent'),
            "wishlists": data.get('wishlists'),
            "itemType": data.get('itemType'),
            "itemCode": data.get('itemCode')
        }
        self.insert_data(GamalyticsMain, main_data)


    def load_data(self):
        # Loop through JSONL files in the data directory and insert data into tables
        self.logger.info("Starting to load data from JSONL files.")
        file_list = [f for f in sorted(os.listdir(self.data_dir)) if f.endswith('.jsonl')]

        for file_name in tqdm(file_list, desc="Files", unit="file", position=0):
            file_path = os.path.join(self.data_dir, file_name)

            # Count lines in the file for inner progress bar
            with open(file_path, 'r') as file:
                total_lines = sum(1 for _ in file)

            with open(file_path, 'r') as f:
                for line in tqdm(f, desc=f"Processing records in {file_name}", total=total_lines, unit="record", position=1):
                    data = json.loads(line)
                    steamId = data["steamId"]

                    self.insert_main_data(data)

                    # insert inter-game relation data
                    self.insert_jsonlist_data(GamalyticsHistory, data, "history", {"steamId": data["steamId"]})
                    self.insert_jsonlist_data(GamalyticsAudienceOverlap, data, "audienceOverlap", {
                        "steamId": steamId, "type": "audience_overlap"
                    })
                    self.insert_jsonlist_data(GamalyticsAudienceOverlap, data, "alsoPlayed", {
                        "steamId": steamId, "type": "also_played"
                    })

                    # insert playtime data
                    playtime_info = data.get("playtimeData", {})
                    for time_range, percentage in playtime_info.get("distribution", {}).items():
                        playtime_data = {
                            "steamId": steamId,
                            "medianPlaytime": playtime_info.get("median"),
                            "timeRange": time_range,
                            "percentage": percentage
                        }
                        self.insert_data(GamalyticsPlaytimeData, playtime_data)
                    
                    # insert estimate details
                    estimate_details = data.get('estimateDetails', {})
                    estimate_data = {
                        "steamId": data['steamId'],
                        "rankBased": estimate_details.get('rankBased'),
                        "playtimeBased": estimate_details.get('playtimeBased'),
                        "reviewBased": estimate_details.get('reviewBased')
                    }
                    self.insert_data(GamalyticsEstimateDetails, estimate_data)

                    # insert DLCs
                    self.insert_related_data(GamalyticsDLC, data, 'dlc', {'steamId': data['steamId']})
                    # insert attributes
                    self.insert_attributes_data(data)

                    # commit change for each game
                    self.session.commit()
        
        self.logger.info("Finished loading Gamalytics data")
            

    def close(self):
        self.session.close()
        self.logger.info("Database session closed for Gamalytics data")

