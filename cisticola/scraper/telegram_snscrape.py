from typing import Generator
from datetime import datetime, timezone
import json
import snscrape.modules
from loguru import logger

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

class TelegramSnscrapeScraper(Scraper):
    """An implementation of a Scraper for Telegram, using snscrape library"""
    __version__ = "TelegramSnscrapeScraper 0.0.1"

    def can_handle(self, channel):
        if channel.platform == "Telegram" and channel.public and not channel.chat:
            return True

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        scr = snscrape.modules.telegram.TelegramChannelScraper(
            channel.screenname)

        g = scr.get_items()

        for post in g:
            if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                logger.info(f'Timestamp of post {post} is earlier than the previous archived timestamp {post.date.replace(tzinfo=timezone.utc)}')
                break

            logger.info(f'Processing post {post}')

            archived_urls = {}

            for image_url in post.images:
                archived_urls[image_url] = None

            for video_url in post.videos:
                archived_urls[video_url] = None

            if archive_media:
                for url in archived_urls:
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Telegram",
                channel=channel.id,
                platform_id=post.url,
                date=post.date,
                date_archived=datetime.now(timezone.utc),
                raw_posts=post.json(),
                archived_urls=archived_urls,
                media_archived=archive_media
            )

    def get_profile(self, channel: Channel) -> RawChannelInfo:

        scr = snscrape.modules.telegram.TelegramChannelScraper(
            channel.screenname)

        profile = scr._get_entity().__dict__
        
        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))
