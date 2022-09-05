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

def scrape_channels_old(args):
    logger.info(f"Scraping old posts from channels, media: {args.media}")

    controller = get_scraper_controller()
    controller.scrape_all_channels(archive_media=args.media, fetch_old=True)

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

def transform(args):
    logger.info(f"Transforming untransformed posts")

    controller = get_transformer_controller()
    controller.transform_all_untransformed()

def transform_info(args):
    logger.info(f"Transforming untransformed channel info")

    controller = get_transformer_controller()
    controller.transform_all_untransformed_info()

def transform_media(args):
    logger.info(f"Transforming untransformed channel media")

    controller = get_transformer_controller()
    controller.transform_all_untransformed_media()

def init_db():
    engine = create_engine(os.environ["DB"])
    mapper_registry.metadata.create_all(bind=engine)


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="DEBUG", catch=True)

    parser = argparse.ArgumentParser(description="Cisticola command line tools")
    parser.add_argument(
        "command",
        type=str,
        help='Command to run: "sync-channels", "scrape-channels", or "archive-media"',
    )
    parser.add_argument(
        "--gsheet", type=str, help="[sync-channels] URL of Google Sheet to synchronize"
    )
    parser.add_argument(
        "--media", action="store_true", help="[scrape-channels] Add this flag to media"
    )
    parser.add_argument("--chronological", action="store_true")
    parser.add_argument("--telethon_session", type=str)

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
    elif args.command == "sync-channels":
        logger.add("logs/sync-channels.log", level="TRACE", rotation="100 MB")
        sync_channels(args, get_db_session())
    elif args.command == "scrape-channels":
        logger.add("logs/scrape-channels.log", level="TRACE", rotation="100 MB")
        scrape_channels(args)
    elif args.command == "scrape-channels-old":
        logger.add("logs/scrape-channels-old.log", level="TRACE", rotation="100 MB")
        scrape_channels_old(args)
    elif args.command == "archive-media":
        logger.add("logs/archive-media.log", level="TRACE", rotation="100 MB")
        archive_media(args)
    elif args.command == "channel-info":
        logger.add("logs/channel-info.log", level="TRACE", rotation="100 MB")
        scrape_channel_info(args)
    elif args.command == "transform":
        logger.add("logs/transform.log", level="TRACE", rotation="100 MB")
        transform(args)
    elif args.command == "transform-info":
        logger.add("logs/transform-info.log", level="TRACE", rotation="100 MB")
        transform_info(args)
    elif args.command == "transform-media":
        logger.add("logs/transform-media.log", level="TRACE", rotation="100 MB")
        transform_media(args)
    else:
        logger.error(f"Unrecognized command {args.command}")
