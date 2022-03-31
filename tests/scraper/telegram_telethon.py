import pytest

from cisticola.base import Channel
from cisticola.scraper import TelegramTelethonScraper

def test_scrape_telegram_telethon_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

@pytest.mark.media
def test_scrape_telegram_telethon_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_telegram_telethon_profile(channel_kwargs):

    scraper = TelegramTelethonScraper()
    channel = Channel(**channel_kwargs['telegram'])
    scraper.get_profile(channel=channel)