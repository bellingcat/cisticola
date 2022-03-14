from cisticola.base import Channel
from cisticola.scraper import InstagramScraper

def test_scrape_instagram_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['instagram'])]
    controller.register_scraper(scraper = InstagramScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_instagram_channel(controller, channel_kwargs):

    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['instagram'])]
    controller.register_scraper(scraper = InstagramScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
