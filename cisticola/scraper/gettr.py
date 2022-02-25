import cisticola.base
import cisticola.scraper.base
from datetime import datetime
import json
from typing import Generator
from gogettr import PublicClient

class GettrScraper(cisticola.scraper.base.Scraper):
    """An implementation of a Scraper for Gettr, using gogettr library"""
    __version__ = "GettrScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split("gettr.com/user/")[1]
        if len(username.split("/")) > 1:
            return None

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        client = PublicClient()
        username = GettrScraper.get_username_from_url(channel.url)
        scraper = client.user_activity(username=username, type="posts")

        for post in scraper:
            if since is not None and datetime.fromtimestamp(post['cdate']*0.001) <= since.date:
                break

            archived_urls = {}

            if 'imgs' in post:
                for img in post['imgs']:
                    url = "https://media.gettr.com/" + img
                    archived_url = self.archive_media(url)
                    archived_urls[img] = archived_url

            if 'main' in post:
                archived_url = self.archive_media("https://media.gettr.com/" + post['main'])
                archived_urls[post['main']] = archived_url

            # TODO this is just archiving the playlist file, not the actual video
            if 'vid' in post:
                archived_url = self.archive_media("https://media.gettr.com/" + post['vid'])
                archived_urls[post['vid']] = archived_url

            yield cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Gettr",
                channel=channel.id,
                platform_id=post['_id'],
                date=datetime.fromtimestamp(post['cdate']/1000.),
                date_archived=datetime.now(),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Gettr" and GettrScraper.get_username_from_url(channel.url) is not None:
            return True
