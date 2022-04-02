from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urlparse
import json 
import re 

from snscrape.modules.vkontakte import VKontakteUserScraper
from loguru import logger
from yt_dlp.extractor.vk import VKIE

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

class VkontakteScraper(Scraper):
    """An implementation of a Scraper for Vkontakte, using snscrape library"""
    __version__ = "VkontakteScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split('https://vk.com/')[1]

        return username

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)
        scraper = VKontakteUserScraper(username)

        first = True

        for post in scraper.get_items():
            if since is not None and datetime.fromordinal(post.date.toordinal()).replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                # with VKontakteUserScraper, the first tweet could be an old pinned tweet
                if first:
                    first = False
                    continue
                else:
                    break

            archived_urls = {}

            if post.photos:

                for photo in post.photos:
                    variant = max(
                        [v for v in photo.variants], key=lambda v: v.width * v.height)
                    url = variant.url
                    if url is not None:
                        archived_urls[url] = None

            if post.video:
                archived_urls[post.video.url] = None

            for url in archived_urls.keys():

                if archive_media:
                    if re.match(VKIE._VALID_URL, url):
                        # Uses regex from yt_dlp to verify VK video URL
                        media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                    else:
                        media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="VK",
                channel=channel.id,
                platform_id=post.url.split('/')[-1],
                date=datetime.fromordinal(post.date.toordinal()).replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_posts=post.json(),
                archived_urls=archived_urls,
                media_archived=datetime.now(timezone.utc) if archive_media else None)

    def archive_files(self, result: ScraperResult) -> ScraperResult:
        for url in result.archived_urls:
            if result.archived_urls[url] is None:
                if re.match(VKIE._VALID_URL, url):
                    # Uses regex from yt_dlp to verify VK video URL
                    media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                else:
                    media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_blob(media_blob, content_type, key)
                result.archived_urls[url] = archived_url

        result.media_archived = datetime.now(timezone.utc)
        return result


    def can_handle(self, channel):
        if channel.platform == "VK":
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        path = urlparse(url).path
        if path.endswith('.jpg'):
            key = '_'.join(path.split('/')[-2:])
        else:
            ext = '.mp4'
            key = path.split('/')[-1] + ext
            
        return key

    def get_profile(self, channel: Channel) -> RawChannelInfo:

        username = self.get_username_from_url(channel.url)
        scraper = VKontakteUserScraper(username)
        
        profile = scraper._get_entity().__dict__

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))
