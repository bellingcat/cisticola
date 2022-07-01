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
from telethon.tl import types
from telethon.helpers import add_surrogate

import os
from datetime import datetime, timezone
from sqlalchemy import func

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Image, Video, Media, Channel


class TelegramTelethonTransformer(Transformer):
    __version__ = 'TelegramTelethonTransformer 0.0.3'

    bad_channels = {}

    def __init__(self):
        super().__init__()

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        # set up a persistent client for Telethon
        self.client =  TelegramClient('transform.session', api_id, api_hash)
        self.client.connect()

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "TelegramTelethonScraper":
            return True

        return False   

    def get_screenname_from_id(self, channel_id):

        try:
            data = self.client.get_entity(channel_id)
            if isinstance(data, types.User):
                return (data.username, str(data.first_name or "") + " " + str(data.last_name or ""), "")
            else:
                return (data.username, data.title, "")
        except ChannelPrivateError:
            logger.info("ChannelPrivateError")
            return ("", "", "ChannelPrivateError")
        except ChannelInvalidError:
            logger.info("ChannelInvalidError")
            return ("", "", "ChannelInvalidError")
        except ValueError:
            logger.info("ValueError")
            return ("", "", "ValueError")

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

        soup = BeautifulSoup(r.content, features = 'lxml')
        post = soup.findAll("div", {"data-post" : orig_screenname + "/" + str(id)})
        name = ""

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
        else:
            fwd_tag = post[0].findAll("a", {"class", "tgme_widget_message_forwarded_from_name"})

            if len(fwd_tag) == 0:
                fwd_tag = post[0].findAll("span", {"class", "tgme_widget_message_forwarded_from_name"})
            
            if len(fwd_tag) >= 1:
                name = fwd_tag[0].text

        return name

    def transform_info(self, data: RawChannelInfo, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        chat_raw = raw['chats'][0]

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw['full_chat']['id'],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=chat_raw['username'],
            name=chat_raw['title'],
            description=raw['full_chat']['about'],
            description_url='', # does not exist for Telegram
            description_location='', # does not exist for Telegram
            followers=raw['full_chat']['participants_count'],
            following=-1, # does not exist for Telegram
            verified=False, #does not exist for Telegram
            date_created=dateutil.parser.parse(chat_raw['date']) if chat_raw['date'] else None,
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)

    def transform(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        if raw['_'] != 'Message':
            # DEBUG
            # logger.warning(f"Cannot convert type {raw['_']} to post")
            return

        fwd_from = None

        # channel = True
        
        # if raw.get('fwd_from') is not None:
        #     if raw['fwd_from'].get('from_id'):
        #         if isinstance(raw['fwd_from']['from_id'], int):
        #             channel_id = str(raw['fwd_from']['from_id'])
        #         else:
        #             if 'channel_id' in raw['fwd_from']['from_id']:
        #                 channel_id = str(raw['fwd_from']['from_id']['channel_id'])
        #             elif 'user_id' in raw['fwd_from']['from_id']:
        #                 channel_id = str(raw['fwd_from']['from_id']['user_id'])
        #     elif raw['fwd_from'].get('channel_id'):
        #         channel_id = str(raw['fwd_from']['channel_id'])
        #     elif raw['fwd_from'].get('user_id'):
        #         channel_id = str(raw['fwd_from']['user_id'])
        #     channel = session.query(Channel).filter_by(platform_id=channel_id, platform = 'Telegram').first()

        #     if channel is None:
        #         (screenname, name, notes) = self.get_screenname_from_id(channel_id)

        #         # if name == "":
        #         #     logger.info("Trying fallback web interface")
        #         #     orig_channel = session.query(Channel).filter_by(id=data.channel).first()
        #         #     if orig_channel.screenname is not None:
        #         #         name = self.get_name_from_web_interface(orig_channel.screenname, raw['id'])

        #         channel = Channel(
        #             name=name,
        #             platform_id=channel_id,
        #             platform=data.platform,
        #             url="https://t.me/s/" + screenname if screenname not in (None, "") else "",
        #             screenname=screenname,
        #             category='forwarded',
        #             source=self.__version__,
        #             notes=notes
        #             )

        #         channel = insert(channel)
        #         logger.info(f"Added {channel}")

        #     fwd_from = channel.id

        reply_to = None
        # reply_to_id = None
        # if raw.get('reply_to'):
        #     reply_to_id = str(raw['reply_to']['reply_to_msg_id'])
        # elif raw.get('reply_to_msg_id'):
        #     reply_to_id = str(raw['reply_to_msg_id'])
        # if reply_to_id:
        #     post = session.query(Post).filter_by(channel=data.channel, platform_id=reply_to_id).first()
        #     if post is None:
        #         reply_to = -1
        #     else:
        #         reply_to = post.id

        mentions = []

        # for mention_entity in [entity for entity in raw['entities'] if entity['_'] == 'MessageEntityMention']:

        #     offset = mention_entity['offset']
        #     length = mention_entity['length']

        #     screenname = add_surrogate(raw['message'])[offset:offset+length].strip('@').strip()

        #     channel = session.query(Channel).filter(func.lower(Channel.screenname)==func.lower(screenname)).first()

        #     if channel is None:

        #         channel = Channel(
        #             name = None,
        #             platform_id = None,
        #             platform = 'Telegram',
        #             url="https://t.me/s/" + screenname,
        #             screenname=screenname,
        #             category='mentioned',
        #             source=self.__version__,
        #             )

        #         channel = insert(channel)
        #         logger.info(f"Added {channel}")

        #     mentions.append(channel.id)

        channel = session.query(Channel).filter_by(id=int(data.channel)).first()

        if channel is not None and channel.url:
            url = channel.url.strip('/') + f"/{raw['id']}"
            author_username = channel.screenname
        else:
            url = ""
            author_username = ""

        transformed = Post(
            raw_id = data.id,
            platform_id = raw['id'],
            scraper = data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=dateutil.parser.parse(raw['date']),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url=url,
            content=add_markdown_links(raw),
            author_id=raw.get('peer_id', {}).get('channel_id'),
            author_username=author_username,
            forwarded_from=fwd_from,
            reply_to=reply_to,
            mentions = mentions,
            forwards = raw.get('forwards'),
            views = raw.get('views')
        )

        # transformed = insert(transformed)
        return transformed

        # for k in data.archived_urls:
        #     if data.archived_urls[k]:
        #         archived_url = data.archived_urls[k]
        #         ext = archived_url.split('.')[-1]

        #         if ext == 'mp4' or ext == 'mov' or ext == 'avi' or ext =='mkv':
        #             insert(Video(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))
        #         else:
        #             insert(Image(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))

def add_markdown_links(raw_post):

    global_offset = 0
    transformed_content = raw_post['message']
    links = [entity for entity in raw_post['entities'] if entity['_'] == 'MessageEntityTextUrl']

    for link in links:
        offset = global_offset + link['offset']
        length = link['length']
        url = link['url']

        before_link = transformed_content[:offset]
        link_text = f"[{transformed_content[offset:offset+length].strip()}]"
        trailing_whitespace = ''.join([c for c in transformed_content[offset:offset+length] if c.isspace()])
        link_href = f"({url})"
        after_link = transformed_content[offset+length:]

        transformed_content = before_link + link_text + link_href + trailing_whitespace + after_link
        global_offset += (4 + len(url))
        
    return transformed_content