import json
import os
from easygoogletranslate import EasyGoogleTranslate
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ETL:
    def __init__(self):
        self.translator = EasyGoogleTranslate(
            target_language='en',
            timeout=10
        )
        self.items = {
            "item_id": [],
            "name": [],
            "image_id": [],
            "product_type": [],
            "style": [],
            "model_name": [],
            "marketplace": [],
            "domain_name": [],
            "brand": [],
            "color": []
        }

        self.bullet_points = {
            "item_id": [],
            "bullet_points": [],
        }

        self.item_keywords = {
            "item_id": [],
            "keywords": [],
        }

    def extract_transform(self, dir_path="product_dec_gen/listings"):
        for listing in os.listdir(dir_path):
            with open(os.path.join(dir_path, listing), "r") as f:
                data = f.readlines()

            logger.info(f"Extracting Data from listing {listing}...")

            for i, line in enumerate(data):
                line_data = json.loads(line)
                self.items['item_id'].append(line_data.get('item_id'))  # Use .get() for optional values
                self.items['name'].append(
                    self.translator.translate(line_data.get('item_name', [{}])[0].get('value', '')))
                self.items['image_id'].append(line_data.get('main_image_id'))
                self.items['product_type'].append(line_data.get('product_type', [{}])[0].get('value', ''))
                self.items['style'].append(line_data.get('style', [{}])[0].get('value', ''))
                self.items['model_name'].append(line_data.get('model_name', [{}])[0].get('value', ''))
                self.items['marketplace'].append(line_data.get('marketplace'))
                self.items['domain_name'].append(line_data.get('domain_name'))
                self.items['brand'].append(self.translator.translate(line_data.get('brand', [{}])[0].get('value', '')))
                self.items['color'].append(self.translator.translate(line_data.get('color', [{}])[0].get('value', '')))

                if line_data.get('bullet_point'):
                    for bullet_point in line_data.get('bullet_point'):
                        self.bullet_points['item_id'].append(line_data['item_id'])
                        self.bullet_points['bullet_points'].append(self.translator.translate(bullet_point['value']))

                if line_data.get('item_keywords'):
                    for keyword in line_data.get('item_keywords'):
                        self.item_keywords['item_id'].append(line_data['item_id'])
                        self.item_keywords['keywords'].append(self.translator.translate(keyword['value']))

                if (i + 1) % 10 == 0 or i == 0:
                    logger.info(f"Checkpoint @ {i + 1}...")
                    self.load()

    def load(self, item_df_path="items.csv",
             bullet_points_df_path="bullet_points.csv",
             item_keywords_df_path="item_keywords.csv"):
        items_df = pd.DataFrame(self.items)
        bullet_points_df = pd.DataFrame(self.bullet_points)
        item_keywords_df = pd.DataFrame(self.item_keywords)

        items_df.to_csv(item_df_path, index=False)
        bullet_points_df.to_csv(bullet_points_df_path, index=False)
        item_keywords_df.to_csv(item_keywords_df_path, index=False)

        logger.info(f"Saved data to CSV files...\n")

    def pipeline(self):
        self.extract_transform()
        self.load()
        logger.info("ETL Pipeline Completed")


etl = ETL()
etl.pipeline()

