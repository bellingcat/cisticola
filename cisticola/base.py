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

@dataclass
class ScraperResult:
    """A minimally processed result from a scraper
    """

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #TODO there is probably a way of making this a Channel object foreign key
    #: User-specified integer that uniquely identifies a channel, e.g. ``15``.
    channel: int

    #: String that uniquely identifies the scraped post on the given platform, e.g. ``"1503397267675533313"``
    platform_id: str

    #: Datetime (relative to UTC) that the scraped post was created at.
    date: datetime

    #: JSON dump of dict that contains all data scraped for the post.
    raw_data: str

    #: Datetime (relative to UTC) that the scraped post was archived at.
    date_archived: datetime

    #: Dict in which the keys are the original media URLs from the post, and the corresponding values are the URLs of the archived media files. 
    archived_urls: dict

@dataclass
class Channel:
    """Information about a specific channel to be scraped.
    """

    #: User-specified integer that uniquely identifies a channel, e.g. ``15``.
    id: int

    #: Name of channel (different from username because it can be non-unique and contain emojis), e.g. ``T🕊Редакция Президент Гордон🕊"``.
    name: str

    #: String that uniquely identifies the channel on the given platform, e.g. ``"-1001101170442"``.
    platform_id: str

    #: User-specified category for the channel, e.g. ``"qanon-adjacent"``.
    category: str

    #: Number of followers the channel has on the given platform, e.e. ``"1465"``.
    followers: int

    #: Name of platform the given channel is on, e.g. ``"Telegram"``.
    platform: str

    #: URL for the given channel on the platform, e.g. ``"https://t.me/prezidentgordonteam"``
    url: str

    #: Screen name/username of channel.
    screenname: str

    #: 2 digit country code for the country of origin for the channel, e.g. ``"RU"``.
    country: str

    #: Name of influencer, if channel belongs to an influencer that operates on multiple platforms.
    influencer: str

    #: Whether or not the channel is publicly-accessible. 
    public: bool

    #: Whether or not the channel is a chat (i.e. allows users who are not the channel creator to post/message)
    chat: bool

    #: Any other additional notes about the channel.
    notes: str

@dataclass
class TransformedResult:
    """An object with fields for columns in the analysis table"""

    #: ID number of the scraped post in the ``raw_data`` table
    raw_id: int

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: String specifying name and version of transformer used to tranform result, e.g. ``"TwitterTransformer 0.0.1"``.
    transformer: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #: User-specified integer that uniquely identifies a channel, e.g. ``15``.
    channel: int

    #: Datetime (relative to UTC) that the scraped post was created at.
    date: datetime

    #: Datetime (relative to UTC) that the scraped post was archived at.
    date_archived: datetime

    #: URL of the original post
    url: str

    #: String that uniquely identifies the channel on the given platform, e.g. ``"-1001101170442"``.
    author_id: str

    #: Username of author who made post.
    author_username: str

    #: Text of the original post
    content: str


mapper_registry = registry()

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