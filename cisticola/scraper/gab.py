import cisticola.base
import cisticola.scraper.base
from datetime import datetime
import json
from typing import Generator, Tuple
from garc import Garc
import tempfile

class GabScraper(cisticola.scraper.base.Scraper):
    """An implementation of a Scraper for Gab, using GARC library"""
    __version__ = "GabScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split('https://gab.com/')[-1]

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        client = Garc(profile = 'main')
        username = GabScraper.get_username_from_url(channel.url)

        scraper = client.userposts(username)

        for post in scraper:
            if since is not None and datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")).replace(tzinfo = None) <= since.date:
                break

            media_urls = []
            archived_urls = {}

            media_urls.extend([p['url'] for p in post['media_attachments']])

            if post.get('repost') is not None:
                media_urls.extend([p['url'] for p in post['repost']['media_attachments']])

            for url in media_urls:
                media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_media(media_blob, content_type, key)
                archived_urls[url] = archived_url

            yield cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Gab",
                channel=channel.id,
                platform_id=post['id'],
                date=datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")).replace(tzinfo = None),
                date_archived=datetime.now(),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Gab" and GabScraper.get_username_from_url(channel.url) is not None:
            return True