from sqlalchemy.orm import sessionmaker, with_polymorphic
import json

from cisticola.base import Channel
from cisticola.scraper import TwitterScraper
from cisticola.transformer import TwitterTransformer
from cisticola.base import TransformedResult, Media

def test_scrape_etl_twitter(engine, controller, etl_controller, channel_kwargs):
    controller.reset_db()
    
    channels = [Channel(**channel_kwargs['twitter'])]
    controller.register_scraper(scraper = TwitterScraper())
    controller.scrape_channels(channels = channels, archive_media = True)

    etl_controller.register_transformer(TwitterTransformer())
    etl_controller.transform_all_untransformed()

    sessionfactory = sessionmaker()
    sessionfactory.configure(bind=engine)
    session = sessionfactory()

    posts = session.query(TransformedResult).all()
    media = session.query(Media).all()

    assert len(posts) == 3
    assert len(media) == 2

    assert posts[-1].content == "This is a test. https://t.co/rzTFL9uFi6"
    assert json.loads(media[-1].exif)['Composite:ImageSize'] == "826 728"