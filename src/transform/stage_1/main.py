from loaders.gamalytics import GamalyticsDataLoader

if __name__ == "__main__":
    db_path = './data/transformed/stage_1.db'
    data_dir = './data/raw/gamalytic/'
    loader = GamalyticsDataLoader(db_path, data_dir)
    loader.load_data()
    loader.close()
