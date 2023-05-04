import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from datetime import datetime, timezone
from sqlalchemy import func

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Image, Video, Media, Channel

class VkontakteTransformer(Transformer):
    """A Vkontakte specific ScraperResult, with a method ETL/transforming"""

    __version__ = "VkontakteTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "VkontakteScraper":
            return True

        return False        

    def transform_info(self, data: RawChannelInfo, insert: Callable, session, channel=None) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw['username'],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=raw['username'],
            name=raw['name'],
            description=raw.get('description'),
            description_url=raw.get('websites'),
            description_location=None,
            followers=int(raw['followers']) if raw['followers'] else None,
            following=-1,
            verified=raw['verified'],
            date_archived=data.date_archived,
            date_created=None,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)


    def transform(self, data: ScraperResult, insert: Callable, session, insert_post, flush_posts) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)           

        transformed = Post(
            raw_id=data.id,
            platform_id=data.platform_id,
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=data.date,
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url=raw['url'],
            content=raw['content'] if raw['content'] else '',
            author_id = None,
            author_username=None,
            outlinks =list(filter(None, raw["outlinks"])) if raw['outlinks'] else [],
            )

        # insert_post
        insert_post(transformed)

        # media = self.process_media(raw, transformed.id, data)
        # for m in media:
        #     insert(m)