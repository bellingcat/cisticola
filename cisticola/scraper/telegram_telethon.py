from typing import Generator
from datetime import datetime, timezone
import os
import json
import tempfile
from pathlib import Path

from loguru import logger
from telethon.sync import TelegramClient 

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

                if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                    logger.info(f'Timestamp of post {post} is earlier than the previous archived timestamp {post.date.replace(tzinfo=timezone.utc)}')
                    break

                post_url = f'{channel.url}/{post.id}'
                key = f'{username}_{post.id}'

                archived_urls = {}

                if archive_media:

                    if post.media is not None:
                        with tempfile.TemporaryDirectory() as temp_dir:
                            output_file = Path(temp_dir, key)
                            client.download_media(post.media, output_file)

                            output_file_with_ext = os.listdir(temp_dir)[0]
                            filename = Path(temp_dir, output_file_with_ext)
                            
                            with open(filename, 'rb') as f:
                                blob = f.read()
                        
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
                    archived_urls=archived_urls)
