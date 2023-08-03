import pytest
from sqlalchemy.sql import text

from cisticola.base import Post, Channel, ChannelInfo, Media,  ScraperResult, RawChannelInfo
from cisticola.scraper import (
    TelegramTelethonScraper, 
    BitchuteScraper, 
    GettrScraper,
    RumbleScraper)
from cisticola.transformer import (
    TelegramTelethonTransformer, 
    BitchuteTransformer, 
    GettrTransformer,
    RumbleTransformer)

CONTROLLERS = {
    'telegram' : {
        'scraper': TelegramTelethonScraper,
        'transformer': TelegramTelethonTransformer
    },
    'bitchute': {
        'scraper': BitchuteScraper,
        'transformer': BitchuteTransformer
    },
    'gettr': {
        'scraper': GettrScraper,
        'transformer': GettrTransformer
    },
    'rumble': {
        'scraper': RumbleScraper,
        'transformer': RumbleTransformer
    }
}
   

@pytest.mark.parametrize('platform', ['telegram','bitchute', 'gettr', 'rumble'])
def test_scraper_and_transformer(platform, session, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    controller.remove_all_scrapers()

    # necessary for comments/replies to be processed correctly
    session.execute(text('INSERT INTO posts(id) VALUES (-1)'))
    session.commit()

    channels = [Channel(**channel_kwargs[platform])]
    scraper = CONTROLLERS[platform]['scraper']
    controller.register_scraper(scraper = scraper())

    controller.scrape_channels(channels = channels)
    controller.scrape_all_channel_info()
    controller.archive_unarchived_media_batch()

    raw_posts = session.query(ScraperResult).all()
    raw_channel_info = session.query(RawChannelInfo).all()
    archived_urls = session.query(ScraperResult.archived_urls).order_by(ScraperResult.date_archived.desc()).first()

    assert len(raw_posts) > 0
    assert len(raw_channel_info) > 0
    assert len(archived_urls) > 0

    controller.remove_all_scrapers()

    transformer = CONTROLLERS[platform]['transformer']

    etl_controller.register_transformer(transformer())
    etl_controller.transform_all_untransformed()
    etl_controller.transform_all_untransformed_info()
    etl_controller.transform_all_untransformed_media()

    posts = session.query(Post).all()
    channel_info = session.query(ChannelInfo).all()
    media = session.query(Media).all()

    assert len(posts) > 0
    assert len(channel_info) > 0
    assert len(media) > 0