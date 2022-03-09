from datetime import datetime, timezone
import json
from typing import Generator

from garc import Garc

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

class GabScraper(Scraper):
    """An implementation of a Scraper for Gab, using GARC library"""
    __version__ = "GabScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split('https://gab.com/')[-1]

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        client = Garc(profile = 'main')
        username = GabScraper.get_username_from_url(channel.url)

        scraper = client.userposts(username)

        for post in scraper:
            if since is not None and datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")) <= since.date:
                break

            media_urls = []
            archived_urls = {}

            if archive_media:

                media_urls.extend([p['url'] for p in post['media_attachments']])

                if post.get('repost') is not None:
                    media_urls.extend([p['url'] for p in post['repost']['media_attachments']])

                for url in media_urls:
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Gab",
                channel=channel.id,
                platform_id=post['id'],
                date=datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Gab" and GabScraper.get_username_from_url(channel.url) is not None:
            return True