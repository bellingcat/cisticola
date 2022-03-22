from sqlalchemy import create_engine
from loguru import logger
import gspread
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cisticola.base import Channel, Post, ScraperResult, mapper_registry
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

logger.add("../test.log")

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
mapper_registry.metadata.create_all(bind=engine)
session_generator = sessionmaker()
session_generator.configure(bind=engine)
session = session_generator()

gc = gspread.service_account(filename='service_account.json')

# Open a sheet from a spreadsheet in one go
wks = gc.open_by_url("https://docs.google.com/spreadsheets/d/1yxd6-2Mp0jZ8r9XJklb39WE-iIMrKRyA2kymJcIfGis/edit#gid=0")
channels = wks.worksheet("channels").get_all_records()

for c in channels:
    del c['followers']

    for k in c.keys():
        if c[k] == 'TRUE': c[k] = True
        if c[k] == 'FALSE': c[k] = False

    # check to see if this already exists, 
    channel = session.query(Channel).filter_by(platform_id=c['platform_id'], platform=c['platform']).first()
    
    if not channel:
        channel = Channel(**c, source='researcher')
        session.add(channel)

session.commit()

controller.connect_to_db(engine)
controller.scrape_all_channels(archive_media = True)

transformer = TwitterTransformer()

etl_controller = ETLController()
etl_controller.register_transformer(transformer)
etl_controller.connect_to_db(engine)
etl_controller.transform_all_untransformed()
