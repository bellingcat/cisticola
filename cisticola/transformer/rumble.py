import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from datetime import datetime, timezone
from sqlalchemy import func, JSON, String, cast, text

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

        if 'id' not in raw:
            # The first version of the Rumble ChannelInfo scraper didn't return
            # the platform_id, so this is a workaround.
            channel = session.query(RawChannelInfo).filter(text("raw_channel_info.raw_data::jsonb ->> 'name'=:name"), RawChannelInfo.platform == 'Rumble').params(name=raw['name']).order_by(RawChannelInfo.date_archived.desc()).first()
            if channel is None:
                platform_id = None
            else:
                platform_id = json.loads(channel.raw_data)['id']
        else:
            platform_id = raw['id']

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=platform_id,
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=platform_id,
            name=raw['name'],
            description='', # does not exist for Rumble
            description_url='', # does not exist for Rumble
            description_location='', # does not exist for Rumble
            followers=_process_number(raw['subscribers']),
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
            likes = _process_number(raw.get('rumbles')),
            video_title = raw['title'],
            video_duration=_parse_duration_str(raw['duration']))

        insert(transformed)

        # media = self.process_media(raw, transformed.id, data)
        # for m in media:
        #     insert(m)

def _process_number(s):

    if s is None:
        return None
    else:
        s = s.replace(' ', '').replace(',','')
        if s.endswith('M'):
            return int(float(s[:-1]) * 1e6)
        elif s.endswith('K'):
            return int(float(s[:-1]) * 1000)
        return int(s)

def _parse_duration_str(duration_str: str) -> int:
    """Convert duration string (e.g. '2:27:04') to the number of seconds (e.g. 8824).
    """
    if not duration_str:
        return None
    else:
        duration_list = duration_str.split(':')
        return sum([int(s) * int(g) for s, g in zip([1, 60, 3600], reversed(duration_list))])