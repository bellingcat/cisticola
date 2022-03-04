from datetime import datetime
import json
from typing import Generator, Tuple
from urllib.parse import urlparse

from gogettr import PublicClient

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper
class GettrScraper(Scraper):
    """An implementation of a Scraper for Gettr, using gogettr library"""
    __version__ = "GettrScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split("gettr.com/user/")[1]
        if len(username.split("/")) > 1:
            return None

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None) -> Generator[ScraperResult, None, None]:
        client = PublicClient()
        username = GettrScraper.get_username_from_url(channel.url)
        scraper = client.user_activity(username=username, type="posts")

        for post in scraper:
            if since is not None and datetime.fromtimestamp(post['cdate']*0.001) <= since.date:
                break

            archived_urls = {}

            if 'imgs' in post:
                for img in post['imgs']:
                    url = "https://media.gettr.com/" + img
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_media(media_blob, content_type, key)
                    archived_urls[img] = archived_url

            if 'main' in post:
                url = "https://media.gettr.com/" + post['main']
                media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_media(media_blob, content_type, key)
                archived_urls[post['main']] = archived_url

            if 'vid' in post:
                url = "https://media.gettr.com/" + post['vid']
                media_blob, content_type, key = self.m3u8_url_to_blob(url)
                archived_url = self.archive_media(media_blob, content_type, key)
                archived_urls[post['vid']] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Gettr",
                channel=channel.id,
                platform_id=post['_id'],
                date=datetime.fromtimestamp(post['cdate']/1000.),
                date_archived=datetime.now(),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Gettr" and GettrScraper.get_username_from_url(channel.url) is not None:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        ext = '.' + content_type.split('/')[-1]
        key = urlparse(url).path.split('/')[-2] + ext
        return key 