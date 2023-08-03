from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urlparse, parse_qs
from snscrape.modules.twitter import TwitterProfileScraper, TwitterUserScraper, Video, Gif, Photo
from loguru import logger
import json

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper, ChannelDoesNotExistError

class TwitterScraper(Scraper):
    """An implementation of a Scraper for Twitter, using snscrape library"""
    __version__ = "TwitterScraper 0.0.0"

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None) -> Generator[ScraperResult, None, None]:
        if channel.platform_id:
            identifier = int(channel.platform_id)
        else:
            identifier = channel.screenname

        scraper = TwitterProfileScraper(identifier)

        first = True

        for tweet in scraper.get_items():
            if since is not None and tweet.date.replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                # with TwitterProfileScraper, the first tweet could be an old pinned tweet
                if first:
                    first = False
                    continue
                else:
                    break

            archived_urls = {}

            media_list = []
            if tweet.media:
                media_list += tweet.media

            if tweet.retweetedTweet and hasattr(tweet.retweetedTweet, 'media') and tweet.retweetedTweet.media:
                media_list += tweet.retweetedTweet.media

            if tweet.quotedTweet and hasattr(tweet.quotedTweet, 'media') and tweet.quotedTweet.media:
                media_list += tweet.quotedTweet.media

            for media in media_list:
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

                if url is not None and url not in archived_urls:
                    archived_urls[url] = None

            yield ScraperResult(
                scraper=self.__version__,
                platform="Twitter",
                channel=channel.id,
                platform_id=tweet.id,
                date=tweet.date,
                date_archived=datetime.now(timezone.utc),
                raw_data=tweet.json(),
                archived_urls=archived_urls,
                media_archived=None)

    def can_handle(self, channel):
        if channel.platform == "Twitter" and (channel.platform_id or channel.screenname):
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        parsed_url = urlparse(url)
        queries = parse_qs(parsed_url.query)

        ext = ''

        # TODO might require additional statements for other media formats
        if 'jpg' in queries.get('format', []):
            ext = '.jpg'
        elif 'png' in queries.get('format', []):
            ext = '.png'
        elif parsed_url.path.endswith('.mp4'):
            ext = ''

        key = parsed_url.path.split('/')[-1] + ext
        return key 

    @logger.catch
    def get_profile(self, channel: Channel) -> RawChannelInfo:

        scraper = TwitterUserScraper(channel.screenname)
        entity = scraper._get_entity()

        if entity is None:
            raise ChannelDoesNotExistError(channel.url)
        else:   
            return RawChannelInfo(scraper=self.__version__,
                platform=channel.platform,
                channel=channel.id,
                raw_data=json.dumps(entity.__dict__, default=str),
                date_archived=datetime.now(timezone.utc))
