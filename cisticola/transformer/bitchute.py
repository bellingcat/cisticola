import json
from loguru import logger
from typing import Generator, Union, Callable
from datetime import datetime, timezone
import dateutil.parser

from bs4 import BeautifulSoup 

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ScraperResult, Post, Image, Video, Media, Channel, ChannelInfo

class BitchuteTransformer(Transformer):
    """A Bitchute specific ScraperResult, with a method ETL/transforming"""

    __version__ = "BitchuteTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "BitchuteScraper":
            return True

        return False        

    def transform_media(self, data: ScraperResult, insert: Callable, transformed: Post) -> Generator[Media, None, None]:
        raw = json.loads(data.raw_data)

        orig = raw['video_url']
        new = data.archived_urls[orig]

        m = Video(url=new, post=transformed.id, raw_id=data.id, original_url=orig)

        insert(m)

    def transform_info(self, data: RawChannelInfo, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw['owner_url'].strip('/').split('/')[-1],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=raw['owner_name'],
            name=raw['owner_name'],
            description=raw['description'],
            description_url='', # does not exist for Bitchute
            description_location='', # does not exist for Bitchute
            followers=raw['subscribers'],
            following=-1, # does not exist for Bitchute
            verified=False, # does not exist for Bitchute
            date_created=dateutil.parser.parse(raw['created']),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)

    def transform(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        soup = BeautifulSoup(raw['body'], features = 'html.parser')
        content = soup.find_all('p')[-1].text

        transformed = Post(
            raw_id=data.id,
            platform_id=raw['id'],
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=data.date,
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url=raw['url'],
            content=content,
            author_id=raw['author_id'],
            author_username=raw['author'])

        transformed = insert(transformed)