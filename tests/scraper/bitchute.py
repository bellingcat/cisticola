from cisticola.base import Channel
from cisticola.scraper import BitchuteScraper

def test_scrape_bitchute_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['bitchute'])]
    controller.register_scraper(BitchuteScraper())
    controller.scrape_channels(channels)
