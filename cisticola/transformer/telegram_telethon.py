import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser
from bs4 import BeautifulSoup
import requests
import time

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

    def get_screenname_from_id(self, orig_screenname, id):
        if orig_screenname in self.bad_channels:
            logger.debug(f"Skipping screenname because it is not accessible for channel {orig_screenname}")
            return ("", "")

        url = "https://t.me/s/" + orig_screenname + "/" + str(id)

        logger.info(f"Finding channel from URL {url}")
        r = requests.get(url)

        if r.url != url:
            self.bad_channels[orig_screenname] = True
            return ("", "")

        soup = BeautifulSoup(r.content)
        post = soup.findAll("div", {"data-post" : orig_screenname + "/" + str(id)})
        if len(post) == 0:
            logger.warning(f"Could not find post from {url}")
            screenname = ""
            name = ""
        else:
            fwd_tag = post[0].findAll("a", {"class", "tgme_widget_message_forwarded_from_name"})

            if len(fwd_tag) > 0:
                fwd_tag = fwd_tag[0]
                name = fwd_tag.text
                screenname = fwd_tag['href'].split('/')[-2]
            else:
                fwd_tag = post[0].findAll("span", {"class", "tgme_widget_message_forwarded_from_name"})
                name = fwd_tag[0].text
                screenname = ""

        return (screenname, name)

    def transform(self, data: ScraperResult, insert: Callable, session) -> Generator[Union[Post, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        if raw['_'] != 'Message':
            logger.warning(f"Cannot convert type {raw['_']} to post")
            return

        fwd_from = None

        if raw['fwd_from'] and raw['fwd_from']['from_id'] and 'channel_id' in raw['fwd_from']['from_id']:
            channel = session.query(Channel).filter_by(platform_id=raw['fwd_from']['from_id']['channel_id']).first()

            if channel is None:
                orig_channel = session.query(Channel).filter_by(id=data.channel).first()
                (screenname, name) = self.get_screenname_from_id(orig_channel.screenname, raw['id'])

                channel = Channel(
                    name=name,
                    platform_id=raw['fwd_from']['from_id']['channel_id'],
                    platform=data.platform,
                    url="https://t.me/s/" + screenname,
                    screenname=screenname,
                    category='forwarded',
                    source=self.__version__
                    )

                channel = insert(channel)
            elif channel.screenname == "":
                # if the screenname is empty, we can fill it in
                orig_channel = session.query(Channel).filter_by(id=data.channel).first()
                (screenname, name) = self.get_screenname_from_id(orig_channel.screenname, raw['id'])

                channel.screenname = screenname
                channel.name = name
                channel.url = "https://t.me/s/" + screenname
                session.flush()

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

        for k in data.archived_urls:
            if data.archived_urls[k]:
                archived_url = data.archived_urls[k]
                ext = archived_url.split('.')[-1]

                if ext == 'mp4' or ext == 'mov' or ext == 'avi' or ext =='mkv':
                    insert(Video(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))
                else:
                    insert(Image(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k))

        