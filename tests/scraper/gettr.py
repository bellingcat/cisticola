from cisticola.base import Channel
from cisticola.scraper import GettrScraper

def test_scrape_gettr_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(scraper = GettrScraper())
    controller.scrape_channels(channels = channels, media = False)

def test_scrape_gettr_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(scraper = GettrScraper())
    controller.scrape_channels(channels = channels, media = True)
