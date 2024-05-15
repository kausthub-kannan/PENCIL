import json
import os
from easygoogletranslate import EasyGoogleTranslate
import pandas as pd
import logging

translator = EasyGoogleTranslate(
    target_language='en',
    timeout=10
)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

items = {
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

bullet_points = {
    "item_id": [],
    "bullet_points": [],
}

item_keywords = {
    "item_id": [],
    "keywords": [],
}

for listing in os.listdir("product_dec_gen/listings"):
    with open(f"product_dec_gen/listings/{listing}", "r") as f:
        data = f.readlines()

    logger.info(f"Extracting Data from listing {listing}...")

    for i, line in enumerate(data):
        line_data = json.loads(line)
        items['item_id'].append(line_data.get('item_id'))  # Use .get() for optional values
        items['name'].append(translator.translate(line_data.get('item_name', [{}])[0].get('value', '')))
        items['image_id'].append(line_data.get('main_image_id'))
        items['product_type'].append(line_data.get('product_type', [{}])[0].get('value', ''))
        items['style'].append(line_data.get('style', [{}])[0].get('value', ''))
        items['model_name'].append(line_data.get('model_name', [{}])[0].get('value', ''))
        items['marketplace'].append(line_data.get('marketplace'))
        items['domain_name'].append(line_data.get('domain_name'))
        items['brand'].append(translator.translate(line_data.get('brand', [{}])[0].get('value', '')))
        items['color'].append(translator.translate(line_data.get('color', [{}])[0].get('value', '')))

        if line_data.get('bullet_point'):
            for bullet_point in line_data.get('bullet_point'):
                bullet_points['item_id'].append(line_data['item_id'])
                bullet_points['bullet_points'].append(translator.translate(bullet_point['value']))

        if line_data.get('item_keywords'):
            for keyword in line_data.get('item_keywords'):
                item_keywords['item_id'].append(line_data['item_id'])
                item_keywords['keywords'].append(translator.translate(keyword['value']))

        if (i + 1) % 10 == 0 or i == 0:
            logger.info(f"Checkpoint @ {i+1}...")
            items_df = pd.DataFrame(items)
            bullet_points_df = pd.DataFrame(bullet_points)
            item_keywords_df = pd.DataFrame(item_keywords)

            items_df.to_csv("items.csv", index=False)
            bullet_points_df.to_csv("bullet_points.csv", index=False)
            item_keywords_df.to_csv("item_keywords.csv", index=False)

    items_df = pd.DataFrame(items)
    bullet_points_df = pd.DataFrame(bullet_points)
    item_keywords_df = pd.DataFrame(item_keywords)

    items_df.to_csv("items.csv", index=False)
    bullet_points_df.to_csv("bullet_points.csv", index=False)
    item_keywords_df.to_csv("item_keywords.csv", index=False)

    logger.info(f"Saved data to CSV files...\n")

logger.info(f"Final Saving...")
items_df = pd.DataFrame(items)
bullet_points_df = pd.DataFrame(bullet_points)
item_keywords_df = pd.DataFrame(item_keywords)

items_df.to_csv("items.csv", index=False)
bullet_points_df.to_csv("bullet_points.csv", index=False)
item_keywords_df.to_csv("item_keywords.csv", index=False)
logging.info(f"Saved data to CSV files successfully\n")
