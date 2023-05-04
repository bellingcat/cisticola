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
from telethon.helpers import add_surrogate, del_surrogate
from itertools import takewhile

import os
from datetime import datetime, timezone
from sqlalchemy import func

from cisticola.transformer.base import Transformer 
from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Image, Video, Audio, Media, Channel


class TelegramTelethonTransformer(Transformer):
    __version__ = 'TelegramTelethonTransformer 0.0.4'

    bad_channels = {}
    channels_cache_by_platformid = {}
    channels_cache_by_id = {}
    channels_cache_by_screenname = {}

    posts_cache = {}

    get_screenname_cache = {}

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "TelegramTelethonScraper":
            return True

        return False   
    
    def __init__(self, telethon_session_name = None):
        super().__init__()

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        if telethon_session_name is None:
            telethon_session_name = phone

        # set up a persistent client for Telethon
        self.client = TelegramClient(telethon_session_name, api_id, api_hash)
        self.client.connect()

    def get_screenname_from_id(self, channel_id):
        if channel_id in self.get_screenname_cache:
            return self.get_screenname_cache[channel_id]
        else:
            output = ("", "", None)
            
            try:
                data = self.client.get_entity(channel_id)
                if isinstance(data, types.User):
                    output = (data.username, str(data.first_name or "") + " " + str(data.last_name or ""), "")
                else:
                    output = (data.username, data.title, "")
            except ChannelPrivateError:
                output = ("", "", "ChannelPrivateError")
            except ChannelInvalidError:
                output = ("", "", "ChannelInvalidError")
            except ValueError:
                output = ("", "", "ValueError")

            self.get_screenname_cache[channel_id] = output
            return output

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

    def transform_info(self, data: RawChannelInfo, insert: Callable, session, channel=None) -> Generator[Union[Post, Channel, Media], None, None]:
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
            date_created=dateutil.parser.parse(chat_raw['date']),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc)
        )

        transformed = insert(transformed)

        if channel.platform_id is None:
            logger.info(f"Missing platform ID on {channel}, setting to {raw['full_chat']['id']}")

            new_channel = session.query(Channel).where(Channel.id == channel.id).one()
            new_channel.platform_id = raw['full_chat']['id']
            session.flush()
            session.commit()

        if len(raw['chats']) > 1:
            for chat in raw['chats'][1:]:
                new_chat = Channel(
                    name=chat["title"],
                    platform_id=chat["id"],
                    category=channel.category, # this should be the same as the "parent"
                    platform=channel.platform, # this should be the same as the "parent"
                    url="",
                    screenname=chat["username"] if "username" in chat else "",
                    country=channel.country, # this should be the same as the "parent"
                    influencer=channel.influencer, # this should be the same as the "parent"
                    public=None,
                    chat=None,
                    notes=channel.id, # this should be the channel ID of the parent
                    source="linked_channel"
                )

                insert(new_chat)

    def transform(self, data: ScraperResult, insert: Callable, session, insert_post, flush_posts) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        if raw['_'] != 'Message':
            logger.warning(f"Cannot convert type {raw['_']} to post")
            return

        fwd_from = None

        if raw['fwd_from'] and raw['fwd_from']['from_id'] and 'channel_id' in raw['fwd_from']['from_id']:
            # use cache rather than a DB request if possible
            if str(raw['fwd_from']['from_id']['channel_id']) not in self.channels_cache_by_platformid:
                channel = session.query(Channel).filter_by(platform_id=str(raw['fwd_from']['from_id']['channel_id']), platform = 'Telegram').first()

                if channel is None:
                    (screenname, name, notes) = self.get_screenname_from_id(raw['fwd_from']['from_id']['channel_id'])

                    if name == "":
                        logger.info("Trying fallback web interface")
                        orig_channel = session.query(Channel).filter_by(id=data.channel).first()
                        if orig_channel.screenname is not None:
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

                self.channels_cache_by_platformid[str(raw['fwd_from']['from_id']['channel_id'])] = channel
            
            fwd_from = self.channels_cache_by_platformid[str(raw['fwd_from']['from_id']['channel_id'])].id

        reply_to = None
        if raw['reply_to']:
            reply_to_id = str(raw['reply_to']['reply_to_msg_id'])
            # use cache rather than a DB request if possible

            if (data.channel, reply_to_id) not in self.posts_cache:
                session.commit()
                flush_posts()
                post = session.query(Post).filter_by(channel=data.channel, platform_id=reply_to_id).first()
                if post is None:
                    reply_to = -1
                else:
                    reply_to = post.id

                self.posts_cache[(data.channel, reply_to_id)] = reply_to
            else:
                reply_to = self.posts_cache[(data.channel, reply_to_id)]

        mentions = []

        for mention_entity in [entity for entity in raw['entities'] if entity['_'] == 'MessageEntityMention']:
            offset = mention_entity['offset']
            length = mention_entity['length']

            screenname = add_surrogate(raw['message'])[offset:offset+length].strip('@').strip()

            # use cache rather than a DB request if possible
            if screenname.lower() not in self.channels_cache_by_screenname:
                channel = session.query(Channel).filter(func.lower(Channel.screenname)==func.lower(screenname)).first()

                if channel is None:

                    channel = Channel(
                        name = None,
                        platform_id = None,
                        platform = 'Telegram',
                        url="https://t.me/s/" + screenname,
                        screenname=screenname,
                        category='mentioned',
                        source=self.__version__,
                        )

                    channel = insert(channel)
                    logger.info(f"Added {channel}")
                
                self.channels_cache_by_screenname[screenname.lower()] = channel
            channel = self.channels_cache_by_screenname[screenname.lower()]

            mentions.append(channel.id)

        # use cache rather than a DB request if possible
        if int(data.channel) not in self.channels_cache_by_id:
            channel = session.query(Channel).filter_by(id=int(data.channel)).first()
            self.channels_cache_by_id[int(data.channel)] = channel

        channel = self.channels_cache_by_id[int(data.channel)]

        if channel is not None and channel.url:
            url = channel.url.strip('/') + f"/{raw['id']}"
            author_username = channel.screenname
        else:
            url = ""
            author_username = ""

        author_id = raw.get('peer_id', {}).get('channel_id')
        if raw['from_id'] and 'user_id' in raw['from_id']:
            author_id = raw['from_id']['user_id']
            author_username = ""
            (screenname, name, notes) = self.get_screenname_from_id(author_id)
            if screenname:
                author_username = screenname

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
            author_id=author_id,
            author_username=author_username,
            forwarded_from=fwd_from,
            reply_to=reply_to,
            mentions = mentions,
            forwards = raw.get('forwards'),
            views = raw.get('views')
        )

        # insert_post
        insert_post(transformed)

def stripped(s):
    """https://stackoverflow.com/a/29933716"""

    lstripped = ''.join(takewhile(str.isspace, s))
    rstripped = ''.join(reversed(tuple(takewhile(str.isspace, reversed(s)))))

    return lstripped + rstripped

def stripped(s):
    """https://stackoverflow.com/a/29933716"""

    lstripped = ''.join(takewhile(str.isspace, s))
    rstripped = ''.join(reversed(tuple(takewhile(str.isspace, reversed(s)))))

    return lstripped + rstripped

def add_markdown_links(raw_post):
    """This function is necessary because Telethon's markdown.unparse doesn't 
    correctly handle trailing whitespace or multi-line links"""

    global_offset = 0
    transformed_content = add_surrogate(raw_post['message'])
    links = [entity for entity in raw_post['entities'] if entity['_'] == 'MessageEntityTextUrl']

    for link in links:
        offset = global_offset + link['offset']
        length = link['length']
        url = link['url']

        before_link = transformed_content[:offset]
        inner_text = transformed_content[offset:offset+length]
        
        # skip creation of link if inner link text is only whitespace
        if inner_text.replace('\u200b', '').strip():
        
            processed_inner_text = inner_text.strip().replace('\n', '\\\n')
            link_text = f"[{processed_inner_text}]"
            trailing_whitespace = stripped(transformed_content[offset:offset+length])
            link_href = f"({url})"
            after_link = transformed_content[offset+length:]

            transformed_content = before_link + link_text + link_href + trailing_whitespace + after_link
            global_offset += (4 + len(url) + inner_text.strip().count('\n'))
        
    return del_surrogate(transformed_content)