from sqlalchemy import create_engine
from loguru import logger

from cisticola.base import Channel, TransformedResult, ScraperResult
from cisticola.scraper import (
    ScraperController,
    BitchuteScraper,
    GabScraper,
    GettrScraper,
    OdyseeScraper,
    RumbleScraper,
    TelegramSnscrapeScraper,
    TelegramTelethonScraper,
    TwitterScraper)
from cisticola.transformer import ETLController
from cisticola.transformer.twitter import TwitterTransformer
from sqlalchemy.orm import sessionmaker

logger.add("../test.log")

test_channels = [
    Channel(
        id=0, 
        name="L Weber (test)", 
        platform_id=1424979017749442595,
        category="test", 
        followers=None, 
        platform="Twitter",
        url="https://twitter.com/LWeber33662141", 
        screenname="LWeber33662141", 
        country="US",
        influencer=None, 
        public=True, 
        chat=False,
        notes="")]

controller = ScraperController()

scrapers = [
    BitchuteScraper(),
    GabScraper(),
    GettrScraper(),
    OdyseeScraper(),
    RumbleScraper(),
    TelegramSnscrapeScraper(),
    TelegramTelethonScraper(),
    TwitterScraper()]

controller.register_scrapers(scrapers)

engine = create_engine('sqlite:///test.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels, archive_media = True)

transformer = TwitterTransformer()

etl_controller = ETLController()
etl_controller.register_transformer(transformer)
etl_controller.connect_to_db(engine)
etl_controller.transform_all_untransformed()
