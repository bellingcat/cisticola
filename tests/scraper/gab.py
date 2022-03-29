import pytest

from cisticola.base import Channel
from cisticola.scraper import GabScraper

def test_scrape_gab_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['gab'])]
    controller.register_scraper(scraper = GabScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_gab_channel(controller, channel_kwargs):
    
    controller.reset_db()

    channels = [Channel(**channel_kwargs['gab'])]
    controller.register_scraper(scraper = GabScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_gab_profile(channel_kwargs):

    scraper = GabScraper()
    channel = Channel(**channel_kwargs['gab'])
    scraper.get_profile(channel=channel)