from cisticola.base import Channel
from cisticola.scraper import OdyseeScraper

def test_scrape_odysee_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['odysee'])]
    controller.register_scraper(scraper = OdyseeScraper())
    controller.scrape_channels(channels = channels, media = False)

def test_scrape_odysee_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['odysee'])]
    controller.register_scraper(scraper = OdyseeScraper())
    controller.scrape_channels(channels = channels, media = True)