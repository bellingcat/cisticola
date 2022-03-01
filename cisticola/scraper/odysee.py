import cisticola.base
import cisticola.scraper.base
from datetime import datetime
import json
from typing import Generator
from polyphemus.base import OdyseeChannel
from urllib.parse import urlparse

class OdyseeScraper(cisticola.scraper.base.Scraper):
    """An implementation of a Scraper for Odysee, using polyphemus library"""
    __version__ = "OdyseeScraper 0.0.1"

    def get_username_from_url(url):

        username = url.split('odysee.com/')[-1].strip('@').split(':')[0]

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:

        username = OdyseeScraper.get_username_from_url(channel.url)
        odysee_channel = OdyseeChannel(channel_name = username)
        
        all_videos = odysee_channel.get_all_videos()

        for video in all_videos:
            if since is not None and datetime.fromtimestamp(video['created']) <= since.date:
                break

            archived_urls = {}
            url = video.info['streaming_url']
            media_blob, content_type, key = self.url_to_blob(url)
            archived_url = self.archive_media(media_blob, content_type, key)
            archived_urls[url] = archived_url

            all_comments = video.get_all_comments()

            yield cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Odysee",
                channel=channel.id,
                platform_id=video.info['claim_id'],
                date=datetime.fromtimestamp(video.info['created']),
                date_archived=datetime.now(),
                raw_data=json.dumps(video.info),
                archived_urls=archived_urls)

            for comment in all_comments:

                yield cisticola.base.ScraperResult(
                    scraper=self.__version__,
                    platform="Odysee",
                    channel=channel.id,
                    platform_id=comment.info['claim_id'],
                    date=datetime.fromtimestamp(comment.info['created']),
                    date_archived=datetime.now(),
                    raw_data=json.dumps(comment.info),
                    archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Odysee" and OdyseeScraper.get_username_from_url(channel.url) is not None:
            return True

    def url_to_key(self, url: str, content_type: str) -> str:
        key = urlparse(url).path.split('/')[-2]
        ext = content_type.split('/')[-1]

        return f'{key}.{ext}'