from cisticola.base import Channel
from cisticola.scraper import GabScraper

def test_scrape_gab_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gab'])]
    controller.register_scraper(GabScraper())
    controller.scrape_channels(channels)
