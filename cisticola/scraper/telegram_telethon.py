from typing import Generator
from datetime import datetime, timezone
import os
import json
import tempfile
from pathlib import Path
import time

from loguru import logger
from telethon.sync import TelegramClient 
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl import types

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

MEDIA_TYPES = ['photo', 'video', 'document', 'webpage']

class TelegramTelethonScraper(Scraper):
    """An implementation of a Scraper for Telegram, using Telethon library"""
    __version__ = "TelegramTelethonScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split('https://t.me/')[1]
        if username.startswith('s/'):
            username = username.split('s/')[1]
        return username

    def archive_files(self, result: ScraperResult, client : TelegramClient = None) -> ScraperResult:
        if len(result.archived_urls.keys()) == 0:
            return result

        if client is None:
            api_id = os.environ['TELEGRAM_API_ID']
            api_hash = os.environ['TELEGRAM_API_HASH']
            phone = os.environ['TELEGRAM_PHONE']

            with TelegramClient(phone, api_id, api_hash) as client:
                return self.archive_files(result, client)

        if len(list(result.archived_urls.keys())) != 1:
            logger.warning(f"Expected 1 key in archived_urls, found {result.archived_keys}")
        else:
            key = list(result.archived_urls.keys())[0]

            if result.archived_urls[key] is None:
                raw = json.loads(result.raw_data)
                    
                message = client.get_messages(raw['peer_id']['channel_id'], ids=[raw['id']])

                blob = None
                if len(message) > 0:
                    blob, output_file_with_ext = self.archive_post_media(message[0], client)
                else:
                    logger.warning("No message retrieved")

                if blob is not None:
                    # TODO specify Content-Type
                    archived_url = self.archive_blob(blob = blob, content_type = '', key = output_file_with_ext)
                    result.archived_urls[key] = archived_url
                    return result
                else:
                    logger.warning("Downloaded blob was None")
            
        result.media_archived = True
        return result

    def archive_post_media(self, post : types.Message, client : TelegramClient = None):
        logger.debug(f"Archiving post {post}")
        
        if post.media is None:
            return None, None
        
        logger.debug(f"Archiving media {post.media}")

        if client is None:
            api_id = os.environ['TELEGRAM_API_ID']
            api_hash = os.environ['TELEGRAM_API_HASH']
            phone = os.environ['TELEGRAM_PHONE']

            with TelegramClient(phone, api_id, api_hash) as client:
                return self.archive_post_media(post, client=client)

        key = f'{post.peer_id.channel_id}_{post.id}'

        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir, key)

            client.download_media(post.media, output_file)

            output_file_with_ext = os.listdir(temp_dir)[0]
            filename = Path(temp_dir, output_file_with_ext)
            
            with open(filename, 'rb') as f:
                blob = f.read()
                return (blob, output_file_with_ext)

    def can_handle(self, channel):
        if channel.platform == "Telegram" and channel.public and not channel.chat:
            return True

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        username = self.get_username_from_url(channel.url)

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        with TelegramClient(phone, api_id, api_hash) as client:
            for post in client.iter_messages(username):
                post_url = f'{channel.url}/{post.id}'

                logger.info(f"Archiving post {post_url} from {post.date}")

                if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                    logger.info(f'Timestamp of post {post} is earlier than the previous archived timestamp {post.date.replace(tzinfo=timezone.utc)}')
                    break

                archived_urls = {}
                logger.info(f"Archiving post {post_url}")

                if post.media is not None:                    
                    archived_urls[post_url] = None

                    if archive_media:
                        blob, output_file_with_ext = self.archive_post_media(post, client)
                        if blob is not None:
                            # TODO specify Content-Type
                            archived_url = self.archive_blob(blob = blob, content_type = '', key = output_file_with_ext)
                            archived_urls[post_url] = archived_url

                yield ScraperResult(
                    scraper=self.__version__,
                    platform="Telegram",
                    channel=channel.id,
                    platform_id=post_url,
                    date=post.date.replace(tzinfo=timezone.utc),
                    date_archived=datetime.now(timezone.utc),
                    raw_data=json.dumps(post.to_dict(), default=str),
                    archived_urls=archived_urls,
                    media_archived=archive_media)

    def get_profile(self, channel: Channel) -> dict:

        username = self.get_username_from_url(channel.url)

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        with TelegramClient(phone, api_id, api_hash) as client:
            full_channel = client(GetFullChannelRequest(channel = username))
        profile = full_channel.__dict__

        return profile
