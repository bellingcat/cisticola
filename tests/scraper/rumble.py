from cisticola.base import Channel
from cisticola.scraper import RumbleScraper

def test_scrape_rumble_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['rumble'])]
    controller.register_scraper(scraper = RumbleScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_rumble_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['rumble'])]
    controller.register_scraper(scraper = RumbleScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
