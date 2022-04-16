import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from bs4 import BeautifulSoup
from psycopg2 import DatabaseError
import requests
import time
from telethon.sync import TelegramClient
from telethon.errors.rpcerrorlist import ChannelPrivateError, ChannelInvalidError
import os

from cisticola.transformer.base import Transformer 
from cisticola.base import ScraperResult, Post, Image, Video, Media, Channel


class TelegramTelethonTransformer(Transformer):
    __version__ = 'TelegramTelethonTransformer 0.0.1'

    bad_channels = {}

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "TelegramTelethonScraper":
            return True

        return False   

    def get_screenname_from_id(self, channel_id):
        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']

        try:
            with TelegramClient("transform.session", api_id, api_hash) as client:
                data = client.get_entity(channel_id)

                return (data.username, data.title, "")
        except ChannelPrivateError:
            logger.info("ChannelPrivateError")
            return ("", "", "ChannelPrivateError")
        except ChannelInvalidError:
            logger.info("ChannelInvalidError")
            return ("", "", "ChannelInvalidError")

    def get_name_from_web_interface(self, orig_screenname, id):
        url = "https://t.me/s/" + orig_screenname + "/" + str(id)

        # this doesn't work for chat channels
        if orig_screenname in self.bad_channels:
            logger.debug(f"Skipping screenname because it is not accessible for channel {orig_screenname}")
            return ""

        logger.info(f"Finding channel from URL {url}")
        r = requests.get(url)

        if r.url != url:
            self.bad_channels[orig_screenname] = True
            return ""

        soup = BeautifulSoup(r.content)
        post = soup.findAll("div", {"data-post" : orig_screenname + "/" + str(id)})

        # multiple posts can be combined into one result in the web interface
        decrement = 0
        while len(post) == 0:
            decrement += 1
            if decrement > 8:
                break

            logger.info(f"Could not find post from {url}, looking for id {id - decrement}")
            post = soup.findAll("div", {"data-post" : orig_screenname + "/" + str(id - decrement)})

        if len(post) == 0:
            logger.warning(f"Could not find post from {url}")
            name = ""
        else:
            fwd_tag = post[0].findAll("a", {"class", "tgme_widget_message_forwarded_from_name"})

            if len(fwd_tag) == 0:
                fwd_tag = post[0].findAll("span", {"class", "tgme_widget_message_forwarded_from_name"})
            
            if len(fwd_tag) >= 1:
                name = fwd_tag[0].text

        return name

    def transform(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        if raw['_'] != 'Message':
            logger.warning(f"Cannot convert type {raw['_']} to post")
            return

        fwd_from = None

        if raw['fwd_from'] and raw['fwd_from']['from_id'] and 'channel_id' in raw['fwd_from']['from_id']:
            channel = session.query(Channel).filter_by(platform_id=str(raw['fwd_from']['from_id']['channel_id'])).first()

            if channel is None:
                (screenname, name, notes) = self.get_screenname_from_id(raw['fwd_from']['from_id']['channel_id'])

                if name == "":
                    logger.info("Trying fallback web interface")
                    orig_channel = session.query(Channel).filter_by(id=data.channel).first()
                    name = self.get_name_from_web_interface(orig_channel.screenname, raw['id'])

                channel = Channel(
                    name=name,
                    platform_id=raw['fwd_from']['from_id']['channel_id'],
                    platform=data.platform,
                    url="https://t.me/s/" + screenname if screenname is not None else "",
                    screenname=screenname,
                    category='forwarded',
                    source=self.__version__,
                    notes=notes
                    )

                channel = insert(channel)
                logger.info(f"Added {channel}")

            fwd_from = channel.id

        reply_to = None
        if raw['reply_to']:
            reply_to_id = raw['reply_to']['reply_to_msg_id']
            post = session.query(Post).filter_by(channel=data.channel, platform_id=reply_to_id).first()
            if post is None:
                reply_to = -1
            else:
                reply_to = post.id

        transformed = Post(
            raw_id = data.id,
            platform_id = raw['id'],
            scraper = data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=dateutil.parser.parse(raw['date']),
            date_archived=data.date_archived,
            url="",
            content=raw['message'],
            author_id=raw['post_author'],
            author_username="",
            forwarded_from=fwd_from,
            reply_to=reply_to
        )

        transformed = insert(transformed)

        # for k in data.archived_urls:
        #     if data.archived_urls[k]:
        #         archived_url = data.archived_urls[k]
        #         ext = archived_url.split('.')[-1]

        #         if ext == 'mp4' or ext == 'mov' or ext == 'avi' or ext =='mkv':
        #             insert(Video(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))
        #         else:
        #             insert(Image(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))

        