from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, JSON, DateTime, ForeignKey

mapper_registry = registry()


@dataclass
class ScraperResult:
    """A minimally processed result from a scraper"""

    scraper: str
    platform: str
    channel: int
    platform_id: str
    date: datetime
    raw_data: str
    date_archived: datetime
    archived_urls: dict


raw_data_table = Table('raw_data', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('scraper', String),
                       Column('platform', String),
                       Column('channel', Integer),
                       Column('platform_id', String),
                       Column('date', DateTime),
                       Column('raw_data', String),
                       Column('date_archived', DateTime),
                       Column('archived_urls', JSON))

mapper_registry.map_imperatively(ScraperResult, raw_data_table)


@dataclass
class Channel:
    id: int
    name: str
    platform_id: str
    category: str
    followers: int
    platform: str
    url: str
    screenname: str
    country: str
    influencer: str
    public: bool
    chat: bool
    notes: str


@dataclass
class TransformedResult:
    """An object with fields for columns in the analysis table"""
    raw_id: int
    scraper: str
    transformer: str
    platform: str
    channel: str
    date: datetime
    date_archived: datetime
    url: str
    content: str
    author_id: str
    author_username: str


analysis_table = Table('analysis', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('raw_id', Integer, ForeignKey('raw_data.id')),
                       Column('scraper', String),
                       Column('transformer', String),
                       Column('platform', String),
                       Column('channel', String),
                       Column('date', DateTime),
                       Column('date_archived', DateTime),
                       Column('url', String),
                       Column('content', String),
                       Column('author_id', String),
                       Column('author_username', String)
                       )

mapper_registry.map_imperatively(TransformedResult, analysis_table)
