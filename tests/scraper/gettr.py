import pytest

from cisticola.base import Channel
from cisticola.scraper import GettrScraper

def test_scrape_gettr_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(scraper = GettrScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_gettr_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(scraper = GettrScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_gettr_profile(channel_kwargs):

    scraper = GettrScraper()
    channel = Channel(**channel_kwargs['gettr'])
    scraper.get_profile(channel=channel)