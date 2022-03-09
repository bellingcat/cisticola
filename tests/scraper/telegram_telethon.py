from cisticola.base import Channel
from cisticola.scraper import TelegramTelethonScraper

def test_scrape_telegram_telethon_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, media = False)
    
def test_scrape_telegram_telethon_channel(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, media = True)
