import pytest

from cisticola.base import Channel
from cisticola.scraper import OdyseeScraper

@pytest.mark.unarchived
def test_scrape_odysee_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['odysee'])]
    controller.register_scraper(scraper = OdyseeScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

@pytest.mark.media
@pytest.mark.unarchived
def test_scrape_odysee_channel_unarchived_media(controller):

    controller.archive_unarchived_media()

@pytest.mark.media
def test_scrape_odysee_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['odysee'])]
    controller.register_scraper(scraper = OdyseeScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_odysee_profile(channel_kwargs):

    scraper = OdyseeScraper()
    channel = Channel(**channel_kwargs['odysee'])
    scraper.get_profile(channel=channel)