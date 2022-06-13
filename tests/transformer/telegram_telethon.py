from sqlalchemy.orm import sessionmaker, with_polymorphic
import json

import pytest

from cisticola.base import Channel
from cisticola.scraper import TelegramTelethonScraper
from cisticola.transformer import TelegramTelethonTransformer
from cisticola.base import Post, Media

@pytest.mark.media
def test_scrape_etl_telegram_telethon(engine, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['telegram'])]
    controller.register_scraper(scraper = TelegramTelethonScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
    controller.scrape_all_channel_info()

    etl_controller.register_transformer(TelegramTelethonTransformer())
    etl_controller.transform_all_untransformed()
    etl_controller.transform_all_untransformed_info()

    sessionfactory = sessionmaker()
    sessionfactory.configure(bind=engine)
    session = sessionfactory()

    posts = session.query(Post).all()
    media = session.query(Media).all()

    assert len(posts) == 19
    # assert len(media) == 13

    assert posts[16].content == "Taking pre-orders now"
    # assert json.loads(media[0].exif)['Composite:ImageSize'] == "1028 1280"