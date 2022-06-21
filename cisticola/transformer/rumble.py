import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from datetime import datetime, timezone

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Image, Video, Media, Channel

class RumbleTransformer(Transformer):
    """A Rumble specific ScraperResult, with a method ETL/transforming"""

    __version__ = "RumbleTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "RumbleScraper":
            return True

        return False        

    def transform_info(self, data: RawChannelInfo, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw['id'],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=raw['id'],
            name=raw['name'],
            description='', # does not exist for Rumble
            description_url='', # does not exist for Rumble
            description_location='', # does not exist for Rumble
            followers=raw['subscribers'],
            following=-1, # does not exist for Rumble
            verified=raw['verified'],
            date_created=None, # does not exist for Rumble
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)


    def transform(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = Post(
            raw_id=data.id,
            platform_id=raw['media_url'].strip('/').split('/')[-1],
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=dateutil.parser.parse(raw['datetime']),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url=raw['link'],
            content=raw['content'],
            author_id=raw['author_id'],
            author_username=raw['author_name'],
            views = _process_number(raw.get('views')),
            likes = _process_number(raw.get('rumbles')))

        insert(transformed)

        # media = self.process_media(raw, transformed.id, data)
        # for m in media:
        #     insert(m)

def _process_number(s):

    if s is None:
        return None
    else:
        return int(s.replace(',', ''))