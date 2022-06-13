from sqlalchemy.orm import sessionmaker
import json

import pytest

from cisticola.base import Channel
from cisticola.scraper import BitchuteScraper
from cisticola.transformer import BitchuteTransformer
from cisticola.base import Post, Media

@pytest.mark.media
def test_scrape_etl_bitchute(engine, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['bitchute'])]
    controller.register_scraper(scraper = BitchuteScraper())
    controller.scrape_channels(channels = channels, archive_media = True)
    controller.scrape_all_channel_info()
    
    etl_controller.register_transformer(BitchuteTransformer())
    etl_controller.transform_all_untransformed()
    etl_controller.transform_all_untransformed_info()

    sessionfactory = sessionmaker()
    sessionfactory.configure(bind=engine)
    session = sessionfactory()

    posts = session.query(Post).all()
    media = session.query(Media).all()

    assert len(posts) == 5
    # assert len(media) == 0

    assert 'Pendant are something that the advanced ladies can fuse in her every day look' in posts[0].content
    # assert json.loads(media[0].exif)['Composite:ImageSize'] == "826 728"