from cisticola.base import Channel
from cisticola.scraper import TelegramSnscrapeScraper

def test_scrape_telegram_snscrape_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram_snscrape'])]
    controller.register_scraper(TelegramSnscrapeScraper())
    controller.scrape_channels(channels)
