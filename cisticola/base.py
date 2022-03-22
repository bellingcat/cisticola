from typing import List
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, JSON, DateTime, ForeignKey, Boolean
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
                       Column('channel', Integer, ForeignKey('channels.id')),
                       Column('platform_id', String),
                       Column('date', DateTime),
                       Column('raw_data', String),
                       Column('date_archived', DateTime),
                       Column('archived_urls', JSON))

mapper_registry.map_imperatively(ScraperResult, raw_data_table)


@dataclass
class Channel:
    name: str
    platform_id: str
    category: str
    platform: str
    url: str
    screenname: str
    country: str = None
    influencer: str = None
    public: bool = None
    chat: bool = None
    notes: str = ""
    source: str = None

    def hydrate(self):
        pass

channel_table = Table('channels', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('name', String),
                    Column('platform_id', Integer),
                    Column('category', String),
                    Column('platform', String),
                    Column('url', String),
                    Column('screenname', String),
                    Column('country', String),
                    Column('influencer', String),
                    Column('public', Boolean),
                    Column('chat', Boolean),
                    Column('notes', String),
                    Column('source', String)
                    )

mapper_registry.map_imperatively(Channel, channel_table)

@dataclass
class Post:
    """An object with fields for columns in the analysis table"""
    raw_id: int
    platform_id: str
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
    forwarded_from: int = None
    reply_to: int = None

    def hydrate(self):
        pass



post_table = Table('posts', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('raw_id', Integer, ForeignKey('raw_data.id')),
                       Column('platform_id', Integer),
                       Column('scraper', String),
                       Column('transformer', String),
                       Column('platform', String),
                       Column('channel', Integer, ForeignKey('channels.id')),
                       Column('date', DateTime),
                       Column('date_archived', DateTime),
                       Column('url', String),
                       Column('author_id', String),
                       Column('author_username', String),
                       Column('content', String),
                       Column('forwarded_from', Integer, ForeignKey('channels.id')),
                       Column('reply_to', Integer, ForeignKey('posts.id'))
                       )

mapper_registry.map_imperatively(Post, post_table)

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
                       Column('post', Integer, ForeignKey('posts.id')),
                       Column('url', String),
                       Column('original_url', String),
                       Column('exif', String),
                       Column('ocr', String)
                       )

mapper_registry.map_imperatively(Media, media_table, polymorphic_on='type', polymorphic_identity='media')
mapper_registry.map_imperatively(Image, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='image')
mapper_registry.map_imperatively(Video, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='video')