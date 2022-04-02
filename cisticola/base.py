from typing import List
from dataclasses import dataclass
from datetime import datetime
import tempfile 
import json
import io

from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, JSON, DateTime, ForeignKey, Boolean
import pytesseract
import PIL
import exiftool

from .utils import make_request

@dataclass
class ScraperResult:
    """A minimally processed result from a scraper
    """

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #: Foreign key of channel ID that this was scraped from
    channel: int

    #: String that uniquely identifies the scraped post on the given platform, e.g. ``"1503397267675533313"``
    platform_id: str

    #: Datetime (relative to UTC) that the scraped post was created at.
    date: datetime

    #: JSON dump of dict that contains all data scraped for the post.
    raw_posts: str

    #: Datetime (relative to UTC) that the scraped post was archived at.
    date_archived: datetime

    #: Dict in which the keys are the original media URLs from the post, and the corresponding values are the URLs of the archived media files. 
    archived_urls: dict

    #: What date was the media archived? (None if not archived)
    media_archived: datetime

@dataclass
class Channel:
    """Information about a specific channel to be scraped.
    """

    #: Name of channel (different from username because it can be non-unique and contain emojis), e.g. ``T🕊Редакция Президент Гордон🕊"``.
    name: str

    #: String that uniquely identifies the channel on the given platform, e.g. ``"-1001101170442"``.
    platform_id: str

    #: User-specified category for the channel, e.g. ``"explicit_qanon"``.
    category: str

    #: Name of platform the given channel is on, e.g. ``"Telegram"``.
    platform: str

    #: URL for the given channel on the platform, e.g. ``"https://t.me/prezidentgordonteam"``
    url: str

    #: Screen name/username of channel.
    screenname: str
      
    #: 2 digit country code for the country of origin for the channel, e.g. ``"RU"``.
    country: str = None
    
    #: Name of influencer, if channel belongs to an influencer that operates on multiple platforms.    
    influencer: str = None
      
    #: Whether or not the channel is publicly-accessible. 
    public: bool = None
      
    #: Whether or not the channel is a chat (i.e. allows users who are not the channel creator to post/message)
    chat: bool = None
      
    #: Any other additional notes about the channel.
    notes: str = ""
      
    #: Did the channel come from a researcher or a scraping process?
    source: str = None

    def hydrate(self):
        pass

@dataclass
class RawChannelInfo:
    """A minimally processed result from a scraper
    """

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #: Foreign key of channel ID that this was scraped from
    channel: int

    #: JSON dump of dict that contains all data scraped for the post.
    raw_data: str

    #: Datetime (relative to UTC) that the scraped post was archived at.
    date_archived: datetime

@dataclass
class Post:
    """An object with fields for columns in the analysis table"""

    #: ID number of the scraped post in the ``raw_posts`` table
    raw_id: int
      
    #: Platform specific post ID
    platform_id: str

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

    #: The ID of the Channel that the post was forwarded or quoted from
    forwarded_from: int = None
      
    #: The ID of the Post that this Post is a reply to or reblog of
    reply_to: int = None

    def hydrate(self):
        pass


@dataclass
class Media:
    """Base class for organizing information about a media file.
    """

    #: ID number of the media's corresponding scraped post in the ``raw_posts`` table.
    raw_id: int

    #: ID number of the media's corresponging scraped post in the ``analysis`` table.
    post: int

    #: URL of the original post.
    url: str

    #: Original URL of the media from the the original post.
    original_url: str

    #: JSON dump of the dict containing metadata information for the media file.
    exif: str = None

    def get_blob(self):
        """Download media file as bytes blob.
        """

        blob = make_request(self.url)
        return blob.content

    def hydrate(self, blob = None):
        """Download media file as bytes blob and extract data from content.
        """

        if blob is None:
            blob = self.get_blob()

        self.hydrate_exif(blob)

    def hydrate_exif(self, blob):
        """Extract Exif metadata from bytes blob.
        """

        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(blob)

            with exiftool.ExifTool() as et:
                exif = et.get_metadata(temp_file.name)
                self.exif = json.dumps(exif)

@dataclass
class Image(Media):
    """Class for organizing information about an image file. 
    """

    #: Extracted OCR content from image
    ocr: str = None

    def hydrate(self, blob=None):
        """Download image file as bytes blob and extract Exif and OCR content 
        from the image.
        """

        if blob is None:
            blob = self.get_blob()

        super().hydrate(blob)
        self.hydrate_ocr(blob)

    def hydrate_ocr(self, blob):
        """Extract OCR (optical character recognition) data from image bytes blob.
        """

        image = PIL.Image.open(io.BytesIO(blob))
        self.ocr = pytesseract.image_to_string(image)

@dataclass
class Video(Media):
    """Class for organizing information about an image file. 
    """
    
    pass

mapper_registry = registry()

raw_posts_table = Table('raw_posts', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('scraper', String),
                       Column('platform', String),
                       Column('channel', Integer, ForeignKey('channels.id')),
                       Column('platform_id', String),
                       Column('date', DateTime),
                       Column('raw_posts', String),
                       Column('date_archived', DateTime),
                       Column('archived_urls', JSON),
                       Column('media_archived', DateTime))

raw_channel_info_table = Table('raw_channel_info', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('scraper', String),
                    Column('platform', String),
                    Column('channel', Integer, ForeignKey('channels.id')),
                    Column('raw_data', String),
                    Column('date_archived', DateTime))

channel_table = Table('channels', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('name', String),
                    Column('platform_id', String),
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

post_table = Table('posts', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('raw_id', Integer, ForeignKey('raw_posts.id')),
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

media_table = Table('media', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('type', String),
                       Column('raw_id', Integer, ForeignKey('raw_posts.id')),
                       Column('post', Integer, ForeignKey('posts.id')),
                       Column('url', String),
                       Column('original_url', String),
                       Column('exif', String),
                       Column('ocr', String))

mapper_registry.map_imperatively(Post, post_table)
mapper_registry.map_imperatively(Channel, channel_table)
mapper_registry.map_imperatively(ScraperResult, raw_posts_table)
mapper_registry.map_imperatively(RawChannelInfo, raw_channel_info_table)
mapper_registry.map_imperatively(Media, media_table, polymorphic_on='type', polymorphic_identity='media')
mapper_registry.map_imperatively(Image, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='image')
mapper_registry.map_imperatively(Video, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='video')