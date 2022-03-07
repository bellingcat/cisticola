from cisticola.base import Channel
from cisticola.scraper import GettrScraper

def test_scrape_gettr_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(GettrScraper())
    controller.scrape_channels(channels)
