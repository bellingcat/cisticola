from cisticola.base import Channel
from cisticola.scraper import TelegramSnscrapeScraper

def test_scrape_telegram_snscrape_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramSnscrapeScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_telegram_snscrape_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramSnscrapeScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
