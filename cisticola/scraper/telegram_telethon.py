from typing import Generator
from datetime import datetime, timezone
import os
import json
import tempfile
from pathlib import Path
import time
import pickle

import requests
from bs4 import BeautifulSoup

from sqlalchemy import func
from loguru import logger
from telethon.sync import TelegramClient 
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl import types

from snscrape.modules.telegram import TelegramChannelScraper

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

MEDIA_TYPES = ['photo', 'video', 'document', 'webpage']

class TelegramTelethonScraper(Scraper):
    """An implementation of a Scraper for Telegram, using Telethon library"""
    __version__ = "TelegramTelethonScraper 0.0.3"
    client = None

    def __init__(self):
        super().__init__()

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        # set up a persistent client for Telethon
        self.client =  TelegramClient('transform.session', api_id, api_hash)
        self.client.connect()

    def __del__(self):
        logger.info("Disconnecting Telethon client")
        self.client.disconnect()

    def get_username_from_url(url):
        username = url.split('https://t.me/')[1]
        if username.startswith('s/'):
            username = username.split('s/')[1]
        return username

    def get_channel_identifier(channel: Channel):
        identifier = channel.platform_id
        
        if identifier is None:
            identifier = channel.screenname
            if identifier is None:
                identifier = TelegramTelethonScraper.get_username_from_url(channel.url)
        else:
            identifier = int(identifier)

        return identifier

    @logger.catch
    def archive_files(self, result: ScraperResult) -> ScraperResult:
        if len(result.archived_urls.keys()) == 0:
            return result

        if len(list(result.archived_urls.keys())) != 1:
            logger.warning(f"Expected 1 key in archived_urls, found {result.archived_keys}")
        else:
            key = list(result.archived_urls.keys())[0]

            if result.archived_urls[key] is None:
                raw = json.loads(result.raw_data)
                    
                message = self.client.get_messages(raw['peer_id']['channel_id'], ids=[raw['id']])

                blob = None
                if len(message) > 0:
                    blob, output_file_with_ext = self.archive_post_media(message[0])
                else:
                    logger.warning("No message retrieved")

                if blob is not None:
                    # TODO specify Content-Type
                    archived_url = self.archive_blob(blob = blob, content_type = '', key = output_file_with_ext)
                    result.archived_urls[key] = archived_url
                    result.media_archived = datetime.now(timezone.utc)
                else:
                    if output_file_with_ext == 'largefile':
                        logger.info("Because this was a large file, not clearing media data")
                        return result

                    logger.warning("Downloaded blob was None")
                    result.archived_urls = {}
                    result.media_archived = datetime.now(timezone.utc)
            
        return result

    def archive_post_media(self, post : types.Message):
        if post.media is None:
            logger.debug("No media for post")
            return None, None
        
        if type(post.media) == types.MessageMediaDocument:
            if post.media.document.size/(1024*1024) > 50:
                logger.info(f"Skipping archive of large {type(post.media)} with size {post.media.document.size/(1024*1024)} MB")
                return (None, "largefile")

            logger.debug(f"Archiving {type(post.media)} with size {post.media.document.size/(1024*1024)} MB")
        else:
            logger.debug(f"Archiving {type(post.media)}")

        key = f'{post.peer_id.channel_id}_{post.id}'

        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir, key)

            self.client.download_media(post.media, output_file)

            if len(os.listdir(temp_dir)) == 0:
                logger.warning(f"No file present. Could not archive {post.media}")
                return None, None

            output_file_with_ext = os.listdir(temp_dir)[0]
            filename = Path(temp_dir, output_file_with_ext)
            
            with open(filename, 'rb') as f:
                blob = f.read()
                return (blob, output_file_with_ext)

    def can_handle(self, channel):
        if channel.platform == "Telegram":
            return True

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        username = TelegramTelethonScraper.get_channel_identifier(channel)

        for post in self.client.iter_messages(username):
            post_url = f'{channel.url}/{post.id}'

            logger.trace(f"Archiving post {post_url} from {post.date}")

            if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                logger.info(f'Timestamp of post {post} is earlier than the previous archived timestamp {post.date.replace(tzinfo=timezone.utc)}')
                break

            archived_urls = {}

            if post.media is not None:                    
                archived_urls[post_url] = None

                # if archive_media:
                #     blob, output_file_with_ext = self.archive_post_media(post, client)
                #     if blob is not None:
                #         # TODO specify Content-Type
                #         archived_url = self.archive_blob(blob = blob, content_type = '', key = output_file_with_ext)
                #         archived_urls[post_url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Telegram",
                channel=channel.id,
                platform_id=post_url,
                date=post.date.replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post.to_dict(), default=str),
                archived_urls=archived_urls,
                media_archived=datetime.now(timezone.utc) if archive_media else None)

    @logger.catch(reraise = True)
    def import_posts(self, file: str, session, insert, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        with open(file, 'rb') as f:
            posts = pickle.load(f)
        screenname = file.split('/')[-1].split('.')[0]
        logger.info(f"Loaded posts from channel {screenname}")
        platform_ids = list(set([p.get('to_id', {}).get('channel_id') for p in posts if p['_'] == 'Message'])) or list(set([p.get('peer_id', {}).get('channel_id') for p in posts if p['_'] == 'Message']))
        if len(platform_ids) > 0:
            platform_id = platform_ids[0]
        else:
            return []
        channel = session.query(Channel).filter_by(platform_id=str(platform_id), platform = 'Telegram').first()
        if channel is None:
            channel = Channel(
                name=None,
                platform_id=platform_id,
                platform='Telegram',
                url="https://t.me/s/" + screenname,
                screenname=screenname,
                category='imported',
                source=self.__version__
                )
            channel = insert(channel)
        else:
            num_posts = session.query(func.count('*')).select_from(ScraperResult).filter(ScraperResult.channel==channel.id).scalar()
            if num_posts != 0:
                logger.info(f"Found {num_posts} already imported for channel {screenname}, skipping")
                return []
        for post in posts:
            post_url = f'{channel.url}/{post["id"]}'

            logger.trace(f"Archiving post {post_url} from {post['date']}")

            archived_urls = {}

            if post.get('media') is not None:           
                archived_urls[post_url] = None

                # if archive_media:
                #     blob, output_file_with_ext = self.archive_post_media(post, client)
                #     if blob is not None:
                #         # TODO specify Content-Type
                #         archived_url = self.archive_blob(blob = blob, content_type = '', key = output_file_with_ext)
                #         archived_urls[post_url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Telegram",
                channel=channel.id,
                platform_id=post_url,
                date=post['date'].replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post, default=str),
                archived_urls=archived_urls,
                media_archived=datetime.now(timezone.utc) if archive_media else None)

    def get_full_channel_tgstat(self, channel):

        username = TelegramTelethonScraper.get_username_from_url(channel.url)
        url = f'https://tgstat.com/channel/@{username}/stat'
        r = requests.get(url, headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'})
        if 'Channel not found' in r.text:
            raise ValueError(f'Channel information not archived')
        
        soup = BeautifulSoup(r.content, features = 'lxml')

        raw_data = {
            'full_chat': {
                'id': channel.platform_id,
                'about':  soup.find('div', class_ = 'col-12 col-sm-7 col-md-8 col-lg-6').text.strip().split('\n')[-1].strip(),
                'participants_count': int(soup.find('h2', class_ = 'text-dark').text.strip().replace(' ', ''))
            },
            'chats': [{
                'username':soup.find('a', {'target': '_blank'}).text.strip().strip('@'),
                'title': soup.find('h1').text.strip(),
                'date': None,
            }],
        }
        
        return raw_data

    def get_full_channel_snscrape(self, channel):
        username = TelegramTelethonScraper.get_username_from_url(channel.url)
        scraper = TelegramChannelScraper(name = username)
        entity = scraper._get_entity()
        raw_data = {
            'full_chat': {
                'id': channel.platform_id,
                'about': entity.description,
                'participants_count': entity.members
            },
            'chats': [{
                'username': entity.username,
                'title': entity.title,
                'date': None,
            }],
        }
        return raw_data

    @logger.catch
    def get_profile(self, channel: Channel) -> RawChannelInfo:
        username = TelegramTelethonScraper.get_channel_identifier(channel)
        try:
            full_channel = self.client(GetFullChannelRequest(channel = username))
            profile = full_channel.to_dict()
        except:
            try:
                profile = self.get_full_channel_snscrape(channel)
            except:
                profile = self.get_full_channel_tgstat(channel)

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile, default=str),
            date_archived=datetime.now(timezone.utc))