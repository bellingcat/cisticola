import cisticola.base
import cisticola.scraper.base
from typing import Generator
import snscrape.modules
from datetime import datetime, timezone


class TelegramSnscrapeScraper(cisticola.scraper.base.Scraper):
    __version__ = "TelegramSnscrapeScraper 0.0.1"

    def can_handle(self, channel):
        if channel.platform == "Telegram" and channel.public and not channel.chat:
            return True

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        scr = snscrape.modules.telegram.TelegramChannelScraper(
            channel.screenname)

        g = scr.get_items()

        for post in g:
            if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                break

            archived_urls = {}

            for image_url in post.images:
                archive_url = self.archive_media(image_url)
                archived_urls[image_url] = archive_url

            if post.video:
                video_archive_url = self.archive_media(post.video)
                archived_urls[post.video] = video_archive_url

            yield cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Telegram",
                channel=channel.id,
                platform_id=post.url,
                date=post.date,
                date_archived=datetime.now(timezone.utc),
                raw_data=post.json(),
                archived_urls=archived_urls
            )
