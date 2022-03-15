from cisticola.base import Channel
from cisticola.scraper import VkontakteScraper

def test_scrape_vkontakte_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['vkontakte'])]
    controller.register_scraper(scraper = VkontakteScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_vkontakte_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['vkontakte'])]
    controller.register_scraper(scraper = VkontakteScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
