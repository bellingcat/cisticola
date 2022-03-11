from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urlparse

from snscrape.modules.vkontakte import VKontakteUserScraper
from loguru import logger

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

class VkontakteScraper(Scraper):
    """An implementation of a Scraper for Vkontakte, using snscrape library"""
    __version__ = "VkontakteScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split('https://vk.com/')[1]

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)
        scraper = VKontakteUserScraper(username)

        first = True

        for post in scraper.get_items():
            if since is not None and post.date.replace(tzinfo=timezone.utc) <= since.date_archived.replace(tzinfo=timezone.utc):
                # with VKontakteUserScraper, the first tweet could be an old pinned tweet
                if first:
                    first = False
                    continue
                else:
                    break

            archived_urls = {}

            if archive_media:

                if post.photos:

                    for photo in post.photos:
                        variant = max(
                            [v for v in photo.variants], key=lambda v: v.width * v.height)
                        url = variant.url
                
                        if url is not None:
                            media_blob, content_type, key = self.url_to_blob(url)
                            archived_url = self.archive_blob(media_blob, content_type, key)
                            archived_urls[url] = archived_url

                if post.video:
                    url = post.video.url
                    media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Vkontatke",
                channel=channel.id,
                platform_id=post.url.split('/')[-1],
                date=datetime.fromordinal(post.date.toordinal()).replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=post.json(),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Vkontakte" and channel.platform_id:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        path = urlparse(url).path
        if path.endswith('.jpg'):
            key = '_'.join(path.split('/')[-2:])
        else:
            ext = '.mp4'
            key = path.split('/')[-1] + ext
            
        return key