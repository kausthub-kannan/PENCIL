from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Items(Base):
    __tablename__ = 'items'

    item_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    image_id = Column(String)
    product_type = Column(String)
    style = Column(String)
    model_name = Column(String)
    marketplace = Column(String)
    domain_name = Column(String)
    brand = Column(String)
    color = Column(String)


class BulletPoint(Base):
    __tablename__ = 'bullet_points'

    id = Column(String, primary_key=True)
    item_id = Column(String, ForeignKey('items.item_id'), nullable=False)
    bullet_points = Column(String, nullable=False)


class ItemKeyword(Base):
    __tablename__ = 'item_keywords'

    id = Column(String, primary_key=True)
    item_id = Column(String, ForeignKey('items.item_id'), nullable=False)
    keywords = Column(String, nullable=False)