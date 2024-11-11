import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.gamalytics import (  # Assuming models are defined in a file named `models.py`
    Base, GamalyticsMain, GamalyticsHistory, GamalyticsAudienceOverlap, 
    GamalyticsPlaytimeData, GamalyticsEstimateDetails, GamalyticsDLC, GamalyticsAttributes
)
import logging
from tqdm import tqdm

class GamalyticsDataLoader:
    def __init__(self, db_path, data_dir):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing the GamalyticsDataLoader")

        self.db_path = db_path
        self.data_dir = data_dir
        self.engine = create_engine(f'sqlite:///{db_path}')
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        Base.metadata.create_all(self.engine)

    def insert_main_data(self, data):
        main_data = GamalyticsMain(
            steamId=data['steamId'],
            name=data.get('name'),
            description=data.get('description'),
            price=data.get('price'),
            reviews=data.get('reviews'),
            reviewsSteam=data.get('reviewsSteam'),
            followers=data.get('followers'),
            avgPlaytime=data.get('avgPlaytime'),
            reviewScore=data.get('reviewScore'),
            releaseDate=data.get('releaseDate'),
            EAReleaseDate=data.get('EAReleaseDate'),
            firstReleaseDate=data.get('firstReleaseDate'),
            earlyAccessExitDate=data.get('earlyAccessExitDate'),
            unreleased=data.get('unreleased'),
            earlyAccess=data.get('earlyAccess'),
            copiesSold=data.get('copiesSold'),
            revenue=data.get('revenue'),
            totalRevenue=data.get('totalRevenue'),
            players=data.get('players'),
            owners=data.get('owners'),
            steamPercent=data.get('steamPercent'),
            wishlists=data.get('wishlists'),
            itemType=data.get('itemType'),
            itemCode=data.get('itemCode')
        )
        self.session.add(main_data)

    def insert_history_data(self, data):
        for entry in data.get('history', []):
            history_data = GamalyticsHistory(
                steamId=data['steamId'],
                timeStamp=entry.get('timeStamp'),
                reviews=entry.get('reviews'),
                price=entry.get('price'),
                score=entry.get('score'),
                players=entry.get('players'),
                avgPlaytime=entry.get('avgPlaytime'),
                sales=entry.get('sales'),
                revenue=entry.get('revenue')
            )
            self.session.add(history_data)

    def insert_audience_overlap_data(self, data):
        for entry in data.get('audienceOverlap', []):
            overlap_data = GamalyticsAudienceOverlap(
                steamId=data['steamId'],
                type='audience_overlap',
                relatedSteamId=entry.get('steamId'),
                link=entry.get('link'),
                relatedName=entry.get('name'),
                relatedReleaseDate=entry.get('releaseDate'),
                relatedPrice=entry.get('price'),
                relatedGenres=', '.join(entry.get('genres', [])),
                relatedCopiesSold=entry.get('copiesSold'),
                relatedRevenue=entry.get('revenue')
            )
            self.session.add(overlap_data)

        for entry in data.get('alsoPlayed', []):
            also_played_data = GamalyticsAudienceOverlap(
                steamId=data['steamId'],
                type='also_played',
                relatedSteamId=entry.get('steamId'),
                link=entry.get('link'),
                relatedName=entry.get('name'),
                relatedReleaseDate=entry.get('releaseDate'),
                relatedPrice=entry.get('price'),
                relatedGenres=', '.join(entry.get('genres', [])),
                relatedCopiesSold=entry.get('copiesSold'),
                relatedRevenue=entry.get('revenue')
            )
            self.session.add(also_played_data)

    def insert_playtime_data(self, data):
        playtime_info = data.get('playtimeData', {})
        for range_key, percentage in playtime_info.get('distribution', {}).items():
            playtime_data = GamalyticsPlaytimeData(
                steamId=data['steamId'],
                medianPlaytime=playtime_info.get('median'),
                timeRange=range_key,
                percentage=percentage
            )
            self.session.add(playtime_data)

    def insert_estimate_details(self, data):
        estimate_info = data.get('estimateDetails', {})
        estimate_data = GamalyticsEstimateDetails(
            steamId=data['steamId'],
            rankBased=estimate_info.get('rankBased'),
            playtimeBased=estimate_info.get('playtimeBased'),
            reviewBased=estimate_info.get('reviewBased')
        )
        self.session.add(estimate_data)

    def insert_dlc_data(self, data):
        for dlc in data.get('dlc', []):
            dlc_data = GamalyticsDLC(
                steamId=data['steamId'],
                dlcSteamId=dlc.get('steamId'),
                dlcName=dlc.get('name'),
                dlcReleaseDate=dlc.get('releaseDate'),
                dlcPrice=dlc.get('price'),
                dlcGenres=', '.join(dlc.get('genres', [])),
                dlcCopiesSold=dlc.get('copiesSold'),
                dlcRevenue=dlc.get('revenue')
            )
            self.session.add(dlc_data)

    def insert_attributes_data(self, data):
        for tag in data.get('tags', []):
            self.session.add(GamalyticsAttributes(steamId=data['steamId'], type='tag', value=tag))
        for genre in data.get('genres', []):
            self.session.add(GamalyticsAttributes(steamId=data['steamId'], type='genre', value=genre))
        for feature in data.get('features', []):
            self.session.add(GamalyticsAttributes(steamId=data['steamId'], type='feature', value=feature))
        for language in data.get('languages', []):
            self.session.add(GamalyticsAttributes(steamId=data['steamId'], type='language', value=language))

    def load_data(self):
        # Loop through JSONL files in the data directory and insert data into tables
        self.logger.info("Starting to load data from JSONL files.")
        file_list = [f for f in sorted(os.listdir(self.data_dir)) if f.endswith('.jsonl')]

        for file_name in tqdm(file_list, desc="Files", unit="file", position=0):
            file_path = os.path.join(self.data_dir, file_name)
            # self.logger.info(f"Beginning processing of {file_path}")

            # Count lines in the file for inner progress bar
            with open(file_path, 'r') as file:
                total_lines = sum(1 for _ in file)

            with open(file_path, 'r') as f:
                for line in tqdm(f, desc=f"Processing records in {file_name}", total=total_lines, unit="record", position=1):
                    data = json.loads(line)
                    self.insert_main_data(data)
                    self.insert_history_data(data)
                    self.insert_audience_overlap_data(data)
                    self.insert_playtime_data(data)
                    self.insert_estimate_details(data)
                    self.insert_dlc_data(data)
                    self.insert_attributes_data(data)
                    
            # Commit all changes
            self.session.commit()
        
        self.logger.info("Finished loading Gamalytics data")
            

    def close(self):
        self.session.close()

