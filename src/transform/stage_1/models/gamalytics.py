from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
import os

Base = declarative_base()

class GamalyticsMain(Base):
    __tablename__ = 'gamalytics_main'
    steamId = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
    price = Column(Float)
    reviews = Column(Integer)
    reviewsSteam = Column(Integer)
    followers = Column(Integer)
    avgPlaytime = Column(Float)
    reviewScore = Column(Integer)
    releaseDate = Column(Integer)
    EAReleaseDate = Column(Integer)
    firstReleaseDate = Column(Integer)
    earlyAccessExitDate = Column(Integer)
    unreleased = Column(Boolean)
    earlyAccess = Column(Boolean)
    copiesSold = Column(Integer)
    revenue = Column(Float)
    totalRevenue = Column(Float)
    players = Column(Integer)
    owners = Column(Integer)
    steamPercent = Column(Float)
    wishlists = Column(Integer)
    itemType = Column(Text)
    itemCode = Column(Integer)

# Historical game data
class GamalyticsHistory(Base):
    __tablename__ = 'gamalytics_history'
    historyId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    timeStamp = Column(Integer)
    reviews = Column(Integer)
    price = Column(Float)
    score = Column(Float)
    players = Column(Float)
    avgPlaytime = Column(Float)
    sales = Column(Integer)
    revenue = Column(Float)

class GamalyticsAudienceOverlap(Base):
    __tablename__ = 'gamalytics_audience_overlap'
    overlapId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    type = Column(Text)  # "audience_overlap" or "also_played"
    relatedSteamId = Column(Integer)
    link = Column(Float)
    relatedName = Column(Text)
    relatedReleaseDate = Column(Integer)
    relatedPrice = Column(Float)
    relatedGenres = Column(Text)
    relatedCopiesSold = Column(Integer)
    relatedRevenue = Column(Float)

class GamalyticsPlaytimeData(Base):
    __tablename__ = 'gamalytics_playtime_data'
    playtimeDataId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    medianPlaytime = Column(Integer)
    timeRange = Column(Text)
    percentage = Column(Float)

class GamalyticsEstimateDetails(Base):
    __tablename__ = 'gamalytics_estimate_details'
    estimateId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    rankBased = Column(Float)
    playtimeBased = Column(Float)
    reviewBased = Column(Float)

class GamalyticsDLC(Base):
    __tablename__ = 'gamalytics_dlc'
    dlcId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    dlcSteamId = Column(Integer)
    dlcName = Column(Text)
    dlcReleaseDate = Column(Integer)
    dlcPrice = Column(Float)
    dlcGenres = Column(Text)
    dlcCopiesSold = Column(Integer)
    dlcRevenue = Column(Float)

class GamalyticsAttributes(Base):
    __tablename__ = 'gamalytics_attributes'
    attributeId = Column(Integer, primary_key=True, autoincrement=True)
    steamId = Column(Integer, ForeignKey('gamalytics_main.steamId'))
    type = Column(Text)  # "tag", "genre", "feature", or "language"
    value = Column(Text)

db_path = './data/transformed/stage_1.db'
os.makedirs(os.path.dirname(db_path), exist_ok=True)

engine = create_engine(f'sqlite:///{db_path}')

Base.metadata.create_all(engine)
