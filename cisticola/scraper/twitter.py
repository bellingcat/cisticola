import cisticola.base
import cisticola.scraper.base
from datetime import datetime, timezone
from typing import Generator
import snscrape.modules
from loguru import logger


class TwitterScraper(cisticola.scraper.base.Scraper):
    """An implementation of a Scraper for Twitter, using snscrape library"""
    __version__ = "TwitterScraper 0.0.1"

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        scraper = snscrape.modules.twitter.TwitterProfileScraper(channel.platform_id)

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
                    if type(media) == snscrape.modules.twitter.Video:
                        variant = max(
                            [v for v in media.variants if v.bitrate], key=lambda v: v.bitrate)
                        url = variant.url
                    elif type(media) == snscrape.modules.twitter.Gif:
                        url = media.variants[0].url
                    elif type(media) == snscrape.modules.twitter.Photo:
                        url = media.fullUrl
                    else:
                        logger.warning(f"Could not get media URL of {media}")
                        url = None

                    if url is not None:
                        archived_url = self.archive_media(url)
                        archived_urls[url] = archived_url

            yield cisticola.base.ScraperResult(
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
