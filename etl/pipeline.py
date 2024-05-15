import json
import os
import logging
import uuid

from easygoogletranslate import EasyGoogleTranslate
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from schema import Items, BulletPoint, ItemKeyword, Base


# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

translator = EasyGoogleTranslate(
            target_language='en',
            timeout=10
        )


# Define the ETL class
class ETL:
    def __init__(self):
        username = 'postgres'
        password = 'llavafinetune'
        host = '4.213.40.12'
        port = 5432
        database = 'postgres'

        try:
            connection_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            engine = create_engine(connection_url)
            Base.metadata.create_all(engine)
            logger.info("Connection successful \n")
        except Exception as e:
            raise Exception(f"Connection to remote SQL failed: {e}")

        self.engine = create_engine(connection_url)
        self.Session = sessionmaker(bind=self.engine)

    def extract_transform_load(self, dir_path="product_dec_gen/listings"):
        for listing in os.listdir(dir_path):
            with open(os.path.join(dir_path, listing), "r") as f:
                data = f.readlines()

            logger.info(f"Extracting Data from listing {listing}...")

            for i, line in enumerate(data):
                line_data = json.loads(line)
                session = self.Session()

                new_item = Items(
                    item_id = line_data.get('item_id'),
                    name = translator.translate(line_data.get('item_name', [{}])[0].get('value', '')),
                    image_id = line_data.get('main_image_id'),
                    product_type = line_data.get('product_type', [{}])[0].get('value', ''),
                    style = line_data.get('style', [{}])[0].get('value', ''),
                    model_name = line_data.get('model_name', [{}])[0].get('value', ''),
                    marketplace = line_data.get('marketplace'),
                    domain_name = line_data.get('domain_name'),
                    brand = translator.translate(line_data.get('brand', [{}])[0].get('value', '')),
                    color = translator.translate(line_data.get('color', [{}])[0].get('value', ''))
                )
                session.add(new_item)
                session.commit()


                if line_data.get('bullet_point'):
                    for bullet_point in line_data.get('bullet_point'):
                        new_bullet_point = BulletPoint(
                            item_id = line_data.get('item_id'),
                            id = uuid.uuid4().hex,
                            bullet_points = translator.translate(bullet_point.get('value'))
                        )
                        session.add(new_bullet_point)
                    session.commit()

                if line_data.get('item_keywords'):
                    for keyword in line_data.get('item_keywords'):
                        new_item_keyword = ItemKeyword(
                            item_id = line_data.get('item_id'),
                            id = uuid.uuid4().hex,
                            keywords = translator.translate(keyword.get('value'))
                        )
                        session.add(new_item_keyword)
                    session.commit()

                if (i+1) % 100 == 0 or (i+1) == 1:
                    logger.info(f"Processed {i} records \n")

    def pipeline(self):
        self.extract_transform_load()
        logger.info("ETL Pipeline Completed")


etl = ETL()
etl.pipeline()

