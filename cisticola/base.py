from typing import List
from dataclasses import dataclass, field
from datetime import datetime
import tempfile 
import json
import io

from sqlalchemy.orm import registry
from sqlalchemy import Table, Column, Integer, String, JSON, DateTime, ForeignKey, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB
import pytesseract
import PIL
import exiftool
import re
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from loguru import logger
import spacy

from .utils import make_request

# Disable decompression bomb check
PIL.Image.MAX_IMAGE_PIXELS = 1024 * 1024 * 256

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
    raw_data: str

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
class ChannelInfo:
    """A processed set of information about a channel.
    """

    # Foreign key from the raw_channel_info table
    raw_channel_info_id: int

    # Foreign ckey from the channels table
    channel: int

    # platform specific ID of the channel
    platform_id: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: String specifying name and version of transformer used to tranform result, e.g. ``"TwitterTransformer 0.0.1"``.
    transformer: str

    #: attributes extracted from the raw channel info object
    screenname: str
    name: str
    description: str
    description_url: str
    description_location: str
    followers: int
    following: int
    verified: bool
    date_created: datetime

    #: Datetime (relative to UTC) that the scraped channel info was archived at.
    date_archived: datetime
    
    #: Datetime (UTC) that the scraped channel info was transformed at.
    date_transformed: datetime

    def hydrate(self):
        pass

nlp_en = spacy.load('en_core_web_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_de = spacy.load('de_core_news_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_it = spacy.load('it_core_news_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_fr = spacy.load('fr_core_news_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_ru = spacy.load('ru_core_news_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_nl = spacy.load('nl_core_news_sm', disable=['parser', 'tok2vec', 'attribute_ruler'])
nlp_xx = spacy.load('xx_ent_wiki_sm')

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

    #: Datetime (UTC) that the scraped post was transformed at.
    date_transformed: datetime
    
    #: URL of the original post
    url: str

    #: String that uniquely identifies the channel on the given platform, e.g. ``"-1001101170442"``.
    author_id: str
    
    #: Username of author who made post.
    author_username: str
      
    #: Text of the original post
    content: str

    #: Named entities detected in post
    named_entities: list = field(default_factory=list)

    #: Any cryptocurrency addresses in post
    cryptocurrency_addresses: list = field(default_factory=list)

    #: Hashtags in post
    hashtags: list = field(default_factory=list)

    #: Links to any other websites
    outlinks: list = field(default_factory=list)

    #: Detected language of post
    detected_language: str = ""

    #: Normalized post content
    normalized_content: str = ""

    #: The ID of the Channel that the post was forwarded or quoted from
    forwarded_from: int = None
      
    #: The ID of the Post that this Post is a reply to
    reply_to: int = None

    #: Other users mentioned in the post
    mentions: list = field(default_factory=list)

    #: Number of positive post reactions (e.g. likes, favorites, rumbles, upvotes, etc.)
    likes: int = None

    #: Number of times the post was forwarded/retweeted/shared
    forwards: int = None

    #: Number of times the post was viewed
    views: int = None

    #: Video title, if post is a video
    video_title: str = None

    #: Video duration in seconds, if post is a video
    video_duration: int = None

    def hydrate(self):
        URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

        # replace is here in order to prevent catastrophic backtracking
        urls = re.findall(URL_REGEX, self.content.replace("::::::::", ""))
        self.outlinks += urls
        self.outlinks =  list(set(outlink for outlink in self.outlinks))

        HASHTAG_REGEX = r"(?:^|\s)[＃#]{1}(\w+)"
        
        hashtags = re.findall(HASHTAG_REGEX, self.content)
        self.hashtags += hashtags
        self.hashtags = list(set(hashtag.lower() for hashtag in self.hashtags))

        # regex patterns for finding crypto addresses
        BTC_REGEX = r'\b(bc(0([ac-hj-np-z02-9]{39}|[ac-hj-np-z02-9]{59})|1[ac-hj-np-z02-9]{8,87})|[13][a-km-zA-HJ-NP-Z1-9]{25,35})\b'
        ETHER_REGEX = r'(0x[a-fA-F0-9]{40})'
        
        self.cryptocurrency_addresses = [m[0] for m in re.findall(BTC_REGEX, self.content)] + re.findall(ETHER_REGEX, self.content)

        try:
            self.detected_language = detect(self.content)
        except LangDetectException:
            self.detected_language = ""

        # Dutch (NL) is often misdetected as Afrikaans (af)
        if self.detected_language == "af":
            self.detected_language = "nl"

        self.hydrate_spacy()

    def hydrate_spacy(self):
        ner_only = False
        
        if self.detected_language == 'en':
            nlp = nlp_en
        elif self.detected_language == 'de':
            nlp = nlp_de
        elif self.detected_language == 'it':
            nlp = nlp_it
        elif self.detected_language == 'fr':
            nlp = nlp_fr
        elif self.detected_language == 'ru':
            nlp = nlp_ru
        elif self.detected_language == 'nl':
            nlp = nlp_nl
        else:
            logger.info(f"No language model for {self.detected_language}")
            nlp = nlp_xx
            ner_only = True

        doc = nlp(self.content)
        
        if not ner_only:
            punctuation = ['?',':','!',',','.',';','|','(',')','--','#','=','+']
            tokens = [t.lemma_ for t in doc if not t.is_stop and t.lemma_ not in punctuation]
            self.normalized_content = ' '.join(tokens)
        else:
            self.normalized_content = ''

        self.named_entities = [{'text': ent.text, 'type': ent.label_} for ent in doc.ents]


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

    #: String specifying name and version of scraper used to generate result, e.g. ``"TwitterScraper 0.0.1"``.
    scraper: str

    #: String specifying name and version of transformer used to tranform result, e.g. ``"TwitterTransformer 0.0.1"``.
    transformer: str

    #: Name of platform from which result was scraped, e.g. ``"Twitter"``.
    platform: str

    #: Datetime (relative to UTC) that the scraped post was created at.
    date: datetime

    #: Datetime (relative to UTC) that the scraped post was archived at.
    date_archived: datetime

    #: Datetime (UTC) that the scraped post was transformed at.
    date_transformed: datetime

    #: JSON dump of the dict containing metadata information for the media file.
    exif: str = None

    def get_blob(self):
        """Download media file as bytes blob.
        """

        blob = make_request(self.url)
        return blob.content

    @logger.catch
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

    @logger.catch
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
    """Class for organizing information about an video file. 
    """
    
    pass

@dataclass
class Audio(Media):
    """Class for organizing information about an audio file. 
    """
    
    pass

mapper_registry = registry()

raw_posts_table = Table('raw_posts', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('scraper', String),
                       Column('platform', String),
                       Column('channel', Integer, ForeignKey('channels.id'), index=True),
                       Column('platform_id', String, index=True),
                       Column('date', DateTime, index=True),
                       Column('raw_data', String),
                       Column('date_archived', DateTime, index=True),
                       Column('archived_urls', JSON),
                       Column('media_archived', DateTime, index=True))

raw_channel_info_table = Table('raw_channel_info', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('scraper', String),
                    Column('platform', String),
                    Column('channel', Integer, ForeignKey('channels.id'), index=True),
                    Column('raw_data', String),
                    Column('date_archived', DateTime, index=True))

channel_info_table = Table('channel_info', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('raw_channel_info_id', Integer, ForeignKey('raw_channel_info.id'), index=True),
                    Column('channel', Integer, ForeignKey('channels.id'), index=True),
                    Column('platform_id', String),
                    Column('scraper', String),
                    Column('transformer', String),
                    Column('platform', String),
                    Column('screenname', String),
                    Column('name', String),
                    Column('description', String),
                    Column('description_url', String),
                    Column('description_location', String),
                    Column('followers', Integer),
                    Column('following', Integer),
                    Column('verified', Boolean),
                    Column('date_created', DateTime),
                    Column('date_archived', DateTime, index=True),
                    Column('date_transformed', DateTime, index=True),
                    )

channel_table = Table('channels', mapper_registry.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('name', String),
                    Column('platform_id', String),
                    Column('category', String),
                    Column('platform', String),
                    Column('url', String),
                    Column('screenname', String),
                    Column('country', JSONB),
                    Column('influencer', String),
                    Column('public', Boolean),
                    Column('chat', Boolean),
                    Column('notes', String),
                    Column('source', String)
                    )

post_table = Table('posts', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('raw_id', Integer, ForeignKey('raw_posts.id'), index=True),
                       Column('platform_id', String, index=True),
                       Column('scraper', String),
                       Column('transformer', String),
                       Column('platform', String),
                       Column('channel', Integer, ForeignKey('channels.id'), index=True),
                       Column('date', DateTime, index=True),
                       Column('date_archived', DateTime, index=True),
                       Column('date_transformed', DateTime, index=True),
                       Column('url', String),
                       Column('author_id', String),
                       Column('author_username', String),
                       Column('content', String),
                       Column('forwarded_from', Integer, ForeignKey('channels.id'), index=True),
                       Column('reply_to', Integer, ForeignKey('posts.id'), index=True),
                       Column('named_entities', JSON),
                       Column('cryptocurrency_addresses', JSON),
                       Column('hashtags', JSON),
                       Column('outlinks', JSON),
                       Column('mentions', JSON),
                       Column('likes', Integer),
                       Column('forwards', Integer),
                       Column('views', Integer),
                       Column('video_title', String),
                       Column('video_duration', Integer),
                       Column('detected_language', String, index = True),
                       Column('normalized_content', String)
                       )

posts_forwarded_from_channel_index = Index('posts_channel_forwarded_from_idx', post_table.c.channel, post_table.c.forwarded_from)

media_table = Table('media', mapper_registry.metadata,
                       Column('id', Integer, primary_key=True,
                              autoincrement=True),
                       Column('type', String),
                       Column('raw_id', Integer, ForeignKey('raw_posts.id'), index=True),
                       Column('post', Integer, ForeignKey('posts.id'), index=True),
                       Column('url', String),
                       Column('original_url', String),
                       Column('exif', String),
                       Column('ocr', String),
                       Column('date', DateTime, index=True),
                       Column('date_archived', DateTime, index=True),
                       Column('date_transformed', DateTime, index=True),
                       Column('scraper', String),
                       Column('transformer', String)
                       )

mapper_registry.map_imperatively(Post, post_table)
mapper_registry.map_imperatively(Channel, channel_table)
mapper_registry.map_imperatively(ScraperResult, raw_posts_table)
mapper_registry.map_imperatively(RawChannelInfo, raw_channel_info_table)
mapper_registry.map_imperatively(ChannelInfo, channel_info_table)
mapper_registry.map_imperatively(Media, media_table, polymorphic_on='type', polymorphic_identity='media')
mapper_registry.map_imperatively(Image, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='image')
mapper_registry.map_imperatively(Video, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='video')
mapper_registry.map_imperatively(Audio, media_table, inherits=Media, polymorphic_on='type', polymorphic_identity='audio')
