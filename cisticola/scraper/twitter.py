import cisticola.base
from datetime import datetime
from typing import List
import snscrape.modules


class TwitterScraper(cisticola.scraper.Scraper):
    """An implementation of a Scraper for Twitter, using snscrape library"""
    __version__ = "TwitterScraper 0.0.1"

    # TODO snscrape should be able to scrape from user ID alone, but there is
    # currently a bug/other issue, so it is extracting the username from URL
    def get_username_from_url(url):
        username = url.split("twitter.com/")[1]
        if len(username.split("/")) > 1:
            return None

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> List[cisticola.base.ScraperResult]:
        posts = []
        scraper = snscrape.modules.twitter.TwitterUserScraper(
            TwitterScraper.get_username_from_url(channel.url))

        for tweet in scraper.get_items():
            if since is not None and tweet.id <= int(since.platform_id):
                break

            posts.append(cisticola.base.ScraperResult(scraper=self.__version__,
                                                      platform="Twitter",
                                                      channel=channel.id,
                                                      platform_id=tweet.id,
                                                      date=tweet.date,
                                                      date_archived=datetime.now(),
                                                      raw_data=tweet.json()))

        return posts

    def can_handle(self, channel):
        if channel.platform == "Twitter" and TwitterScraper.get_username_from_url(channel.url) is not None:
            return True
