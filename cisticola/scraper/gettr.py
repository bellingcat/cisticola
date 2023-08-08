import json
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlparse

from gogettr import PublicClient
from loguru import logger

from cisticola.base import Channel, RawChannelInfo, ScraperResult
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
    def get_posts(
        self, channel: Channel, since: Optional[ScraperResult] = None
    ) -> Generator[ScraperResult, None, None]:
        client = PublicClient()
        username = self.get_username_from_url(channel.url).lower()
        scraper = client.user_activity(username=username, type="posts")

        for post in scraper:
            if (
                since is not None
                and datetime.fromtimestamp(post["cdate"] * 0.001) <= since.date
            ):
                break

            archived_urls = {}

            if "imgs" in post:
                for img in post["imgs"]:
                    url = "https://media.gettr.com/" + img
                    archived_urls[url] = None

            if "main" in post:
                url = "https://media.gettr.com/" + post["main"]
                archived_urls[url] = None

            if "ovid" in post:
                url = "https://media.gettr.com/" + post["ovid"]
                archived_urls[url] = None

            yield ScraperResult(
                scraper=self.__version__,
                platform="Gettr",
                channel=channel.id,
                platform_id=post["_id"],
                date=datetime.fromtimestamp(post["cdate"] / 1000.0),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post),
                archived_urls=archived_urls,
                media_archived=None,
            )

    def can_handle(self, channel):
        if (
            channel.platform == "Gettr"
            and self.get_username_from_url(channel.url) is not None
        ):
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        ext = "." + content_type.split("/")[-1]
        key = urlparse(url).path.split("/")[-2] + ext
        return key

    @logger.catch
    def get_profile(self, channel: Channel) -> RawChannelInfo:
        client = PublicClient()
        username = self.get_username_from_url(channel.url)
        profile = client.user_info(username)

        return RawChannelInfo(
            scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc),
        )
