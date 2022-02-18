from dataclasses import dataclass
from datetime import datetime
from typing import List
from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, DateTime
from loguru import logger

mapper_registry = registry()


@dataclass
class ScraperResult:
    """A minimally processed result from a scraper"""
    scraper: str
    platform: str
    channel: str
    platform_id: str
    date: datetime
    raw_data: str
    date_archived: datetime


raw_data_table = Table('raw_data', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True),
                       Column('scraper', String),
                       Column('platform', String),
                       Column('channel', String),
                       Column('platform_id', String),
                       Column('date', DateTime),
                       Column('raw_data', String),
                       Column('date_archived', DateTime))

mapper_registry.map_imperatively(ScraperResult, raw_data_table)


class Scraper:
    __version__ = "Scraper 0.0.1"

    def __init__(self):
        pass

    def __str__(self):
        return self.__version__

    def can_handle(self, channel) -> bool:
        pass

    def get_posts(self, channel, since=None) -> List[ScraperResult]:
        pass


@dataclass
class Channel:
    id: int
    name: str
    platform_id: str
    category: str
    followers: int
    platform: str
    url: str
    country: str
    influencer: str
    public: bool
    chat: bool
    notes: str
