from typing import Generator
from datetime import datetime, timezone

import snscrape.modules
from loguru import logger

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

class TelegramSnscrapeScraper(Scraper):
    __version__ = "TelegramSnscrapeScraper 0.0.1"

    def can_handle(self, channel):
        if channel.platform == "Telegram" and channel.public and not channel.chat:
            return True

    def get_posts(self, channel: Channel, since: ScraperResult = None, media: bool = True) -> Generator[ScraperResult, None, None]:
        scr = snscrape.modules.telegram.TelegramChannelScraper(
            channel.screenname)

        g = scr.get_items()

        for post in g:
            if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                logger.info(f'Timestamp of post {post} is earlier than the previous archived timestamp {post.date.replace(tzinfo=timezone.utc)}')
                break

            logger.info(f'Processing post {post}')

            archived_urls = {}

            if media:

                for image_url in post.images:
                    logger.debug(f'Archiving image: {image_url}')
                    media_blob, content_type, key = self.url_to_blob(image_url)
                    archived_url = self.archive_media(media_blob, content_type, key)
                    archived_urls[image_url] = archived_url

                if post.video:
                    logger.debug(f'Archiving video: {post.video}')
                    media_blob, content_type, key = self.url_to_blob(post.video)
                    archived_url = self.archive_media(media_blob, content_type, key)
                    archived_urls[post.video] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Telegram",
                channel=channel.id,
                platform_id=post.url,
                date=post.date,
                date_archived=datetime.now(timezone.utc),
                raw_data=post.json(),
                archived_urls=archived_urls
            )
