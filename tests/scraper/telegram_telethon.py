import pytest

from cisticola.base import Channel
from cisticola.scraper import TelegramTelethonScraper

@pytest.mark.unarchived
def test_scrape_telegram_telethon_channel_no_media(controller, channel_kwargs):
    controller.remove_all_scrapers()

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

@pytest.mark.media
@pytest.mark.unarchived
def test_scrape_telegram_telethon_unarchived_media(controller):

    controller.archive_unarchived_media_batch()

@pytest.mark.media
def test_scrape_telegram_telethon_channel(controller, channel_kwargs):

    controller.reset_db()
    controller.remove_all_scrapers()
    
    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

@pytest.mark.profile
def test_scrape_telegram_telethon_profile(controller, channel_kwargs):

    controller.reset_db()
    controller.remove_all_scrapers()

    scraper = TelegramTelethonScraper()
    channel = Channel(**channel_kwargs['telegram'])
    scraper.get_profile(channel=channel)