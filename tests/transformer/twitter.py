from sqlalchemy.orm import sessionmaker
import json

import pytest

from cisticola.base import Channel
from cisticola.scraper import TwitterScraper
from cisticola.transformer import TwitterTransformer
from cisticola.base import Post, Media

@pytest.mark.media
def test_scrape_etl_twitter(engine, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

    etl_controller.register_transformer(TwitterTransformer())
    etl_controller.transform_all_untransformed()
    etl_controller.transform_all_untransformed_info()

    sessionfactory = sessionmaker()
    sessionfactory.configure(bind=engine)
    session = sessionfactory()

    posts = session.query(Post).all()
    media = session.query(Media).all()

    assert len(posts) == 12
    assert len(media) == 4

    assert posts[2].content == "BARN"
    assert json.loads(media[0].exif)['Composite:ImageSize'] == "826 728"