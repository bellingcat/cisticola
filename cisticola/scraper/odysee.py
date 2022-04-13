from datetime import datetime, timezone
import json
from typing import Generator
from urllib.parse import urlparse

import requests
from loguru import logger

from polyphemus.base import OdyseeChannelScraper, process_raw_comment_info
from polyphemus.api import get_auth_token, get_all_comments
from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

class OdyseeScraper(Scraper):
    """An implementation of a Scraper for Odysee, using polyphemus library"""
    __version__ = "OdyseeScraper 0.0.0"

    def __init__(self):
        super().__init__()
        self.auth_token = get_auth_token()

    def get_username_from_url(self, url):

        username = url.split('odysee.com/')[-1].strip('@').split(':')[0]

        return username

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)
        scraper = OdyseeChannelScraper(channel_name = username, auth_token = self.auth_token)
        
        all_videos = scraper.get_all_videos()

        for video in all_videos:
            if since is not None and datetime.fromtimestamp(video.created) <= since.date:
                break

            url = video.streaming_url
            if url is None:
                archived_urls = {}
            else:
                archived_urls = {url: None}

                if archive_media:

                    # Check if file is a video file or an m3u8 file
                    r = requests.head(url)
                    if r.headers['Content-Type'] == 'text/html; charset=utf-8':
                        media_blob, content_type, key = self.m3u8_url_to_blob(url)
                    else:
                        media_blob, content_type, key = self.url_to_blob(url)

                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            raw_comment_info_list = get_all_comments(video_id=video.claim_id)
            all_comments = (process_raw_comment_info(raw_comment_info) for raw_comment_info in raw_comment_info_list)

            yield ScraperResult(
                scraper=self.__version__,
                platform="Odysee",
                channel=channel.id,
                platform_id=video.claim_id,
                date=datetime.fromtimestamp(video.created),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(video.__dict__),
                archived_urls=archived_urls,
                media_archived=datetime.now(timezone.utc) if archive_media else None)

            for comment in all_comments:

                yield ScraperResult(
                    scraper=self.__version__,
                    platform="Odysee",
                    channel=channel.id,
                    platform_id=comment.claim_id,
                    date=datetime.fromtimestamp(comment.created),
                    date_archived=datetime.now(),
                    raw_data=json.dumps(comment.__dict__),
                    archived_urls={},
                    media_archived=datetime.now(timezone.utc))

    @logger.catch
    def archive_files(self, result: ScraperResult) -> ScraperResult:
        for url in result.archived_urls:
            if result.archived_urls[url] is None:
                r = requests.head(url)
                if r.headers['Content-Type'] == 'text/html; charset=utf-8':
                    media_blob, content_type, key = self.m3u8_url_to_blob(url)
                else:
                    media_blob, content_type, key = self.url_to_blob(url)

                archived_url = self.archive_blob(media_blob, content_type, key)
                result.archived_urls[url] = archived_url

        result.media_archived = datetime.now(timezone.utc)
        return result

    def can_handle(self, channel):
        if channel.platform == "Odysee" and self.get_username_from_url(channel.url) is not None:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        key = urlparse(url).path.split('/')[-2]
        ext = content_type.split('/')[-1]

        return f'{key}.{ext}'

    def get_profile(self, channel: Channel) -> RawChannelInfo:

        username = self.get_username_from_url(channel.url)
        scraper = OdyseeChannelScraper(channel_name = username, auth_token = self.auth_token)
        profile = scraper.get_entity().__dict__

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))