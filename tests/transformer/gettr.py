from sqlalchemy.orm import sessionmaker
import json

import pytest

from cisticola.base import Channel
from cisticola.scraper import GettrScraper
from cisticola.transformer import GettrTransformer
from cisticola.base import Post, Media

@pytest.mark.media
def test_scrape_etl_gettr(engine, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['gettr'])]
    controller.register_scraper(scraper = GettrScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

    etl_controller.register_transformer(GettrTransformer())
    etl_controller.transform_all_untransformed()
    etl_controller.transform_all_untransformed_info()

    sessionfactory = sessionmaker()
    sessionfactory.configure(bind=engine)
    session = sessionfactory()

    posts = session.query(Post).all()
    media = session.query(Media).all()

    assert len(posts) == 23
    # assert len(media) == 0

    assert 'Nigerian gender studies' in posts[-1].content
    # assert json.loads(media[0].exif)['Composite:ImageSize'] == "826 728"