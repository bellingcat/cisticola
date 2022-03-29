import pytest

from cisticola.base import Channel
from cisticola.scraper import YoutubeScraper

def test_scrape_youtube_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['youtube'])]
    controller.register_scraper(scraper = YoutubeScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_youtube_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['youtube'])]
    controller.register_scraper(scraper = YoutubeScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_youtube_profile(channel_kwargs):

    scraper = YoutubeScraper()
    channel = Channel(**channel_kwargs['youtube'])
    scraper.get_profile(channel=channel)