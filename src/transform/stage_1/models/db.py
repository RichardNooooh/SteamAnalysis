from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
import os

BASE = declarative_base()

DB_PATH = './data/transformed/stage_1.db'
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

ENGINE = create_engine(f'sqlite:///{DB_PATH}')
Base.metadata.create_all(ENGINE)
