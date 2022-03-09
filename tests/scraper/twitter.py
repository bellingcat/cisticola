from cisticola.base import Channel
from cisticola.scraper import TwitterScraper

def test_scrape_twitter_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_twitter_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
