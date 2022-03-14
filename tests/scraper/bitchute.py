from cisticola.base import Channel
from cisticola.scraper import BitchuteScraper

def test_scrape_bitchute_channel_no_media(controller, channel_kwargs):

    channels = [Channel(**channel_kwargs['bitchute'])]
    controller.register_scraper(scraper = BitchuteScraper())
    controller.scrape_channels(channels = channels, archive_media = False)

def test_scrape_bitchute_channel(controller, channel_kwargs):

    controller.reset_db()

    channels = [Channel(**channel_kwargs['bitchute'])]
    controller.register_scraper(scraper = BitchuteScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
