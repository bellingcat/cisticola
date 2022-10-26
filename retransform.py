import argparse
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import sys

from cisticola.base import mapper_registry
from cisticola.scraper import (
    ScraperController,
    VkontakteScraper,
    TelegramTelethonScraper,
    GettrScraper,
    BitchuteScraper,
    RumbleScraper,
)
from cisticola.transformer import (
    ETLController,
    TelegramTelethonTransformer,
    GettrTransformer,
    RumbleTransformer,
    BitchuteTransformer,
    VkontakteTransformer,
)

from sync_with_gsheet import sync_channels

def get_db_session():
    engine = create_engine(os.environ["DB"])

    session_generator = sessionmaker()
    session_generator.configure(bind=engine)
    session = session_generator()

    return session


def get_scraper_controller(telethon_session_name = None):
    engine = create_engine(os.environ["DB"])

    controller = ScraperController()
    controller.connect_to_db(engine)

    scrapers = [VkontakteScraper(),
        TelegramTelethonScraper(telethon_session_name = telethon_session_name),
        GettrScraper(),
        BitchuteScraper(),
        RumbleScraper()]

    controller.register_scrapers(scrapers)

    return controller

def get_transformer_controller():
    engine = create_engine(os.environ["DB"])

    controller = ETLController()
    controller.connect_to_db(engine)

    transformers = [VkontakteTransformer(),
        TelegramTelethonTransformer(),
        GettrTransformer(),
        BitchuteTransformer(),
        RumbleTransformer()]

    controller.register_transformers(transformers)

    return controller


def scrape_channels(args):
    logger.info(f"Scraping channels, media: {args.media}")

    controller = get_scraper_controller()
    controller.scrape_all_channels(archive_media=args.media)


def scrape_channel_info(args):
    logger.info(f"Scraping channel info")

    controller = get_scraper_controller()
    controller.scrape_all_channel_info()


def archive_media(args):
    logger.info(f"Archiving unarchived media")

    if args.telethon_session:
        controller = get_scraper_controller(telethon_session_name=args.telethon_session)
    else:
        controller = get_scraper_controller()
    
    if args.chronological:
        controller.archive_unarchived_media(chronological=True)
    else:
        controller.archive_unarchived_media()

def retransform():
    logger.info(f"Transforming untransformed posts")

    controller = get_transformer_controller()
    controller.retransform_all(query_kwargs = {'channel': 6})

def init_db():
    engine = create_engine(os.environ["DB"])
    mapper_registry.metadata.create_all(bind=engine)


if __name__ == "__main__":

    retransform()


