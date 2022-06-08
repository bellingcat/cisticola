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

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

MEDIA_TYPES = ['photo', 'video', 'document', 'webpage']

class TelegramTelethonScraper(Scraper):
    """An implementation of a Scraper for Telegram, using Telethon library"""
    __version__ = "TelegramTelethonScraper 0.0.2"

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
                    result.media_archived = datetime.now(timezone.utc)
                else:
                    if output_file_with_ext == 'largefile':
                        logger.info("Because this was a large file, not clearing media data")
                        return result

                    logger.warning("Downloaded blob was None")
                    result.archived_urls = {}
                    result.media_archived = datetime.now(timezone.utc)
            
        return result

    def archive_post_media(self, post : types.Message, client : TelegramClient = None):
        if post.media is None:
            logger.debug("No media for post")
            return None, None
        
        if client is None:
            api_id = os.environ['TELEGRAM_API_ID']
            api_hash = os.environ['TELEGRAM_API_HASH']
            phone = os.environ['TELEGRAM_PHONE']

            with TelegramClient(phone, api_id, api_hash) as client:
                return self.archive_post_media(post, client=client)

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

            client.download_media(post.media, output_file)

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

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        with TelegramClient(phone, api_id, api_hash) as client:
            for post in client.iter_messages(username):
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

    @logger.catch
    def get_profile(self, channel: Channel) -> RawChannelInfo:
        username = TelegramTelethonScraper.get_channel_identifier(channel)

        api_id = os.environ['TELEGRAM_API_ID']
        api_hash = os.environ['TELEGRAM_API_HASH']
        phone = os.environ['TELEGRAM_PHONE']

        with TelegramClient(phone, api_id, api_hash) as client:
            full_channel = client(GetFullChannelRequest(channel = username))
        profile = full_channel.to_dict()

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile, default=str),
            date_archived=datetime.now(timezone.utc))
