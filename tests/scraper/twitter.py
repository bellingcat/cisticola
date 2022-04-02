import pytest

from cisticola.base import Channel
from cisticola.scraper import TwitterScraper

@pytest.mark.unarchived
def test_scrape_twitter_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

@pytest.mark.media
@pytest.mark.unarchived
def test_scrape_twitter_channel_unarchived_media(controller):

    controller.archive_unarchived_media()

@pytest.mark.media
def test_scrape_twitter_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_twitter_profile(channel_kwargs):

    scraper = TwitterScraper()
    channel = Channel(**channel_kwargs['twitter'])
    scraper.get_profile(channel=channel)