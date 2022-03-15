from datetime import datetime, timezone
import json
from typing import Generator
from urllib.parse import urlparse

from polyphemus.base import OdyseeChannel
import requests

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

class OdyseeScraper(Scraper):
    """An implementation of a Scraper for Odysee, using polyphemus library"""
    __version__ = "OdyseeScraper 0.0.1"

    def get_username_from_url(self, url):

        username = url.split('odysee.com/')[-1].strip('@').split(':')[0]

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)
        odysee_channel = OdyseeChannel(channel_name = username)
        
        all_videos = odysee_channel.get_all_videos()

        for video in all_videos:
            if since is not None and datetime.fromtimestamp(video['created']) <= since.date:
                break

            archived_urls = {}

            if archive_media:
                url = video.info['streaming_url']

                # Check if file is a video file or an m3u8 file
                r = requests.head(url)
                if r.headers['Content-Type'] == 'text/html; charset=utf-8':
                    media_blob, content_type, key = self.m3u8_url_to_blob(url)
                else:
                    media_blob, content_type, key = self.url_to_blob(url)

                archived_url = self.archive_blob(media_blob, content_type, key)
                archived_urls[url] = archived_url

            all_comments = video.get_all_comments()

            yield ScraperResult(
                scraper=self.__version__,
                platform="Odysee",
                channel=channel.id,
                platform_id=video.info['claim_id'],
                date=datetime.fromtimestamp(video.info['created']),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(video.info),
                archived_urls=archived_urls)

            for comment in all_comments:

                yield ScraperResult(
                    scraper=self.__version__,
                    platform="Odysee",
                    channel=channel.id,
                    platform_id=comment.info['claim_id'],
                    date=datetime.fromtimestamp(comment.info['created']),
                    date_archived=datetime.now(),
                    raw_data=json.dumps(comment.info),
                    archived_urls={})

    def can_handle(self, channel):
        if channel.platform == "Odysee" and self.get_username_from_url(channel.url) is not None:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        key = urlparse(url).path.split('/')[-2]
        ext = content_type.split('/')[-1]

        return f'{key}.{ext}'