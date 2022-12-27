import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from datetime import datetime, timezone
from sqlalchemy import func
from gogettr import PublicClient
from gogettr.api import GettrApiError

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Image, Video, Media, Channel

class GettrTransformer(Transformer):
    """A Gettr specific ScraperResult, with a method ETL/transforming"""

    __version__ = "GettrTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "GettrScraper":
            return True

        return False        

    def transform_info(self, data: RawChannelInfo, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw['_id'],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=raw['username'],
            name=raw['nickname'],
            description=raw.get('dsc'),
            description_url=raw.get('website'),
            description_location=raw.get('location'),
            followers=int(raw['flg']),
            following=int(raw['flw']),
            verified=True if raw.get('infl') else False,
            date_created=datetime.fromtimestamp(int(raw['cdate'])*0.001),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)

    def _get_channel_id(self, username: str, category: str, insert: Callable, session):

        channel = session.query(Channel).where((func.lower(Channel.screenname)==func.lower(username)) & (Channel.platform == 'Gettr')).first()

        if channel is None:
            try:
                client = PublicClient()
                profile = client.user_info(username.lower())
                screenname = profile.get('_id')
                channel = Channel(
                    name=profile.get('nickname'),
                    platform_id=screenname,
                    platform='Gettr',
                    url="https://gettr.com/user/" + screenname,
                    screenname=screenname,
                    category=category,
                    source=self.__version__,
                    )
            except GettrApiError:
                channel = Channel(
                    name = None,
                    platform_id = None,
                    platform = 'Gettr',
                    url = None,
                    screenname=username,
                    category=category,
                    source=self.__version__,
                    notes='GettrApiError'
                    )

            channel = insert(channel)

        return channel.id

    def get_transformed_post(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        if raw["activity"]["action"] == "shares_pst":
            forwarded_from = self._get_channel_id(
                username = str(raw["activity"]["uid"]), category = 'forwarded', insert = insert, session = session)
        else:
            forwarded_from = None

        mentions = []
        for mentioned_user in raw.get("utgs", []):
            mentioned_id = self._get_channel_id(
                username = mentioned_user, category = 'mentioned', insert = insert, session = session)
            mentions.append(mentioned_id)
            
        transformed = Post(
            raw_id=data.id,
            platform_id=raw["_id"],
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=datetime.fromtimestamp(raw["activity"]["cdate"] / 1000.0),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url="https://www.gettr.com/post/" + raw["_id"],
            content=raw.get("txt", ""),
            author_id=raw["receiver_id"],
            author_username=raw["uid"],
            hashtags=raw.get("htgs", []),
            outlinks = list(filter(None, [raw.get("prevsrc")])),
            forwarded_from = forwarded_from,
            mentions = mentions,
            likes = raw.get('lkbpst'),
            forwards = raw.get("shbpst"),
            views = raw.get('vfpst')
            )

        insert(transformed)

        # media = self.process_media(raw, transformed.id, data)
        # for m in media:
        #     insert(m)