from sqlalchemy.orm import sessionmaker
from ..models.db import ENGINE
import logging

class BaseLoader:
    def __init__(self, data_folder: str):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
        self.logger = logging.getLogger(__name__)

        self.data_folder = data_folder

        Session = sessionmaker(bind=ENGINE)
        self.session = Session()

    @abstractmethod
    def load_data():
        pass

