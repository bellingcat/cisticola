from typing import List
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, JSON, DateTime, ForeignKey
import pytesseract
import PIL
import io
import exiftool
import json
import os

from .utils import make_request

mapper_registry = registry()

@dataclass
class ScraperResult:
    """A minimally processed result from a scraper"""

    scraper: str
    platform: str
    channel: int #TODO there is probably a way of making this a Channel object foreign key
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
    channel: int
    date: datetime
    date_archived: datetime
    url: str
    author_id: str
    author_username: str
    content: str



analysis_table = Table('analysis', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('raw_id', Integer, ForeignKey('raw_data.id')),
                       Column('scraper', String),
                       Column('transformer', String),
                       Column('platform', String),
                       Column('channel', Integer),
                       Column('date', DateTime),
                       Column('date_archived', DateTime),
                       Column('url', String),
                       Column('author_id', String),
                       Column('author_username', String),
                       Column('content', String)
                       )

mapper_registry.map_imperatively(TransformedResult, analysis_table)

@dataclass
class Media:
    raw_id: int
    post: int
    url: str
    original_url: str

    exif: str = None

    def get_blob(self):
        blob = make_request(self.url)
        return blob.content

    def hydrate(self, blob = None):
        if blob is None:
            blob = self.get_blob()

        self.hydrate_exif(blob)

    def hydrate_exif(self, blob):
        f = open('tmp', 'wb')
        f.write(blob)
        f.close()

        with exiftool.ExifTool() as et:
            exif = et.get_metadata('tmp')
            self.exif = json.dumps(exif)

        os.remove('tmp')

@dataclass
class Image(Media):
    ocr: str = None

    def hydrate(self, blob=None):
        if blob is None:
            blob = self.get_blob()

        super().hydrate(blob)
        self.hydrate_ocr(blob)

    def hydrate_ocr(self, blob):
        image = PIL.Image.open(io.BytesIO(blob))
        self.ocr = pytesseract.image_to_string(image)

@dataclass
class Video(Media):
    pass

media_table = Table('media', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                        Column('type', String),
                       Column('raw_id', Integer, ForeignKey('raw_data.id')),
                       Column('post', Integer, ForeignKey('analysis.id')),
                       Column('url', String),
                       Column('original_url', String),
                       Column('exif', String),
                       Column('ocr', String)
                       )

mapper_registry.map_imperatively(Media, media_table, polymorphic_on='type', polymorphic_identity='media')
mapper_registry.map_imperatively(Image, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='image')
mapper_registry.map_imperatively(Video, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='video')