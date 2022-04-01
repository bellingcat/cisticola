from datetime import datetime, timezone
import json
from typing import Generator
from urllib.parse import urlparse
from loguru import logger

from gogettr import PublicClient

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

class GettrScraper(Scraper):
    """An implementation of a Scraper for Gettr, using gogettr library"""
    __version__ = "GettrScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split("gettr.com/user/")[1]
        if len(username.split("/")) > 1:
            return None

        return username

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        client = PublicClient()
        username = self.get_username_from_url(channel.url)
        scraper = client.user_activity(username=username, type="posts")

        for post in scraper:
            if since is not None and datetime.fromtimestamp(post['cdate']*0.001) <= since.date:
                break

            archived_urls = {}

            if 'imgs' in post:
                for img in post['imgs']:
                    url = "https://media.gettr.com/" + img
                    archived_urls[url] = None

            if 'main' in post:
                url = "https://media.gettr.com/" + post['main']
                archived_urls[url] = None

            if 'ovid' in post:
                url = "https://media.gettr.com/" + post['ovid']
                archived_urls[url] = None

            for url in archived_urls.keys():

                if archive_media:
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Gettr",
                channel=channel.id,
                platform_id=post['_id'],
                date=datetime.fromtimestamp(post['cdate']/1000.),
                date_archived=datetime.now(timezone.utc),
                raw_posts=json.dumps(post),
                archived_urls=archived_urls,
                media_archived=archive_media)

    def can_handle(self, channel):
        if channel.platform == "Gettr" and self.get_username_from_url(channel.url) is not None:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        ext = '.' + content_type.split('/')[-1]
        key = urlparse(url).path.split('/')[-2] + ext
        return key 

    def get_profile(self, channel: Channel) -> RawChannelInfo:
        client = PublicClient()
        username = self.get_username_from_url(channel.url)
        profile = client.user_info(username)

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))
