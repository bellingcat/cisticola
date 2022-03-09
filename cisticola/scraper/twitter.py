from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urlparse, parse_qs

from snscrape.modules.twitter import TwitterProfileScraper, Video, Gif, Photo
from loguru import logger

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

class TwitterScraper(Scraper):
    """An implementation of a Scraper for Twitter, using snscrape library"""
    __version__ = "TwitterScraper 0.0.1"

    def get_posts(self, channel: Channel, since: ScraperResult = None, media: bool = True) -> Generator[ScraperResult, None, None]:
        scraper = TwitterProfileScraper(channel.platform_id)

        first = True

        for tweet in scraper.get_items():
            if since is not None and tweet.date.replace(tzinfo=timezone.utc) <= since.date_archived.replace(tzinfo=timezone.utc):
                # with TwitterProfileScraper, the first tweet could be an old pinned tweet
                if first:
                    first = False
                    continue
                else:
                    break

            archived_urls = {}

            if tweet.media:
                for media in tweet.media:
                    if type(media) == Video:
                        variant = max(
                            [v for v in media.variants if v.bitrate], key=lambda v: v.bitrate)
                        url = variant.url
                    elif type(media) == Gif:
                        url = media.variants[0].url
                    elif type(media) == Photo:
                        url = media.fullUrl
                    else:
                        logger.warning(f"Could not get media URL of {media}")
                        url = None

                    if url is not None:
                        media_blob, content_type, key = self.url_to_blob(url)
                        archived_url = self.archive_media(media_blob, content_type, key)
                        archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Twitter",
                channel=channel.id,
                platform_id=tweet.id,
                date=tweet.date,
                date_archived=datetime.now(),
                raw_data=tweet.json(),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Twitter" and channel.platform_id:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        parsed_url = urlparse(url)
        queries = parse_qs(parsed_url.query)

        # TODO might require additional statements for other media formats
        if 'jpg' in queries.get('format', []):
            ext = '.jpg'
        elif parsed_url.path.endswith('.mp4'):
            ext = ''

        key = parsed_url.path.split('/')[-1] + ext
        return key 