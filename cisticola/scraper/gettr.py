import cisticola.base
from datetime import datetime
import json
from typing import List
from gogettr import PublicClient

class GettrScraper(cisticola.scraper.Scraper):
    """An implementation of a Scraper for Gettr, using gogettr library"""
    __version__ = "GettrScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split("gettr.com/user/")[1]
        if len(username.split("/")) > 1:
            return None

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> List[cisticola.base.ScraperResult]:
        posts = []
        client = PublicClient()
        username = GettrScraper.get_username_from_url(channel.url)
        scraper = client.user_activity(username=username, type="posts")

        for post in scraper:
            if since is not None and post['cdate'] <= int(since.date_archived.timestamp()):
                break

            posts.append(cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Gettr",
                channel=username,
                platform_id=post['_id'],
                date=datetime.fromtimestamp(post['cdate']/1000.),
                date_archived=datetime.now(),
                raw_data=json.dumps(post)))

        return posts

    def can_handle(self, channel):
        if channel.platform == "Gettr" and GettrScraper.get_username_from_url(channel.url) is not None:
            return True
