import argparse
import datetime
import os
import sys

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cisticola.base import mapper_registry
from cisticola.scraper import (
    BitchuteScraper,
    GettrScraper,
    RumbleScraper,
    ScraperController,
    TelegramTelethonScraper,
)
from cisticola.transformer import (
    BitchuteTransformer,
    ETLController,
    GettrTransformer,
    RumbleTransformer,
    TelegramTelethonTransformer,
)
from sync_with_gsheet import sync_channels


def get_db_session():
    engine = create_engine(os.environ["DB"])

    session_generator = sessionmaker()
    session_generator.configure(bind=engine)
    session = session_generator()

    return session


def get_scraper_controller(args):
    engine = create_engine(os.environ["DB"])

    controller = ScraperController()
    controller.connect_to_db(engine)

    if args.telethon_session:
        telethon_session_name = args.telethon_session
    else:
        telethon_session_name = None

    scrapers = [  # VkontakteScraper(),
        TelegramTelethonScraper(telethon_session_name=telethon_session_name),
        GettrScraper(),
        BitchuteScraper(),
        RumbleScraper(),
    ]

    controller.register_scrapers(scrapers)

    return controller


def get_transformer_controller(args):
    engine = create_engine(os.environ["DB"])

    controller = ETLController()
    controller.connect_to_db(engine)

    if args.telethon_session:
        telethon_session_name = args.telethon_session
    else:
        telethon_session_name = None

    transformers = [  # VkontakteTransformer(),
        TelegramTelethonTransformer(telethon_session_name=telethon_session_name),
        GettrTransformer(),
        BitchuteTransformer(),
        RumbleTransformer(),
    ]

    controller.register_transformers(transformers)

    return controller


def scrape_channels(args):
    logger.info("Scraping channels")

    controller = get_scraper_controller(args)
    controller.scrape_all_channels()


def scrape_channels_old(args):
    logger.info("Scraping old posts from channels")

    controller = get_scraper_controller(args)
    controller.scrape_all_channels(fetch_old=True)


def scrape_channel_info(args):
    logger.info("Scraping channel info")

    controller = get_scraper_controller(args)
    controller.scrape_all_channel_info()


def archive_media(args):
    logger.info("Archiving unarchived media")

    controller = get_scraper_controller(args)

    if args.chronological:
        controller.archive_unarchived_media(chronological=True)
    else:
        controller.archive_unarchived_media()


def transform(args):
    logger.info("Transforming untransformed posts")

    controller = get_transformer_controller(args)

    if args.min_date:
        min_date = datetime.datetime.fromisoformat(args.min_date)
    else:
        min_date = datetime.datetime(1970, 1, 1)

    controller.transform_all_untransformed(min_date=min_date)


def transform_info(args):
    logger.info("Transforming untransformed channel info")

    controller = get_transformer_controller(args)
    controller.transform_all_untransformed_info()

    # sync_channels(args, get_db_session())


def transform_media(args):
    logger.info("Transforming untransformed channel media")

    controller = get_transformer_controller(args)
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
    parser.add_argument("--chronological", action="store_true")
    parser.add_argument("--telethon_session", type=str)
    parser.add_argument("--min_date", type=str)

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
    elif args.command == "sync-channels":
        logger.add(
            "logs/sync-channels.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        sync_channels(args, get_db_session())
    elif args.command == "scrape-channels":
        logger.add(
            "logs/scrape-channels.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        scrape_channels(args)
    elif args.command == "scrape-channels-old":
        logger.add(
            "logs/scrape-channels-old.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        scrape_channels_old(args)
    elif args.command == "archive-media":
        logger.add(
            "logs/archive-media.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        archive_media(args)
    elif args.command == "channel-info":
        logger.add(
            "logs/channel-info.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        scrape_channel_info(args)
    elif args.command == "transform":
        logger.add(
            "logs/transform.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        logger.add("logs/transform_trace.log", level="TRACE", retention="7 days")
        transform(args)
    elif args.command == "transform-info":
        logger.add(
            "logs/transform-info.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        transform_info(args)
    elif args.command == "transform-media":
        logger.add(
            "logs/transform-media.log",
            level="DEBUG",
            rotation="100 MB",
            retention="2 weeks",
            compression="zip",
        )
        transform_media(args)
    else:
        logger.error(f"Unrecognized command {args.command}")
