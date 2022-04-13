import argparse
from loguru import logger
import gspread
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import os
import time
import sys

from cisticola.base import Channel, mapper_registry
from cisticola.scraper import (
    ScraperController,
    VkontakteScraper,
    TelegramTelethonScraper,
    GettrScraper,
    BitchuteScraper,
    RumbleScraper,
)


def sync_channels(args):
    logger.info("Synchronizing channels")

    session = get_db_session()

    gc = gspread.service_account(filename="service_account.json")

    # Open a sheet from a spreadsheet in one go
    wks = gc.open_by_url(args.gsheet).worksheet("channels")
    channels = wks.get_all_records()
    row = 2

    for c in channels:
        if c["public"] == "":
            c["public"] = False
        if c["chat"] == "":
            c["chat"] = False

        for k in c.keys():
            if c[k] == "TRUE" or c[k] == "yes":
                c[k] = True
            if c[k] == "FALSE" or c[k] == "no":
                c[k] = False

            if c[k] == "":
                c[k] = None

        del c["followers"]

        # add new channel
        if c["id"] == "" or c["id"] is None:
            del c["id"]

            # check to see if this already exists,
            platform_id = None
            if c["platform_id"] != "":
                platform_id = c["platform_id"]

            channel = (
                session.query(Channel)
                .filter_by(
                    platform_id=str(platform_id), platform=c["platform"], url=c["url"]
                )
                .first()
            )

            if not channel:
                channel = Channel(**c, source="researcher")
                logger.debug(f"{channel} does not exist, adding")
                session.add(channel)
                session.flush()
                session.commit()

                wks.update_cell(row, 1, channel.id)
                time.sleep(1)
        else:
            channel = session.query(Channel).filter_by(id=int(c["id"])).first()

            logger.info(f"Updating channel {channel}")
            channel.name = c["name"]
            channel.category = c["category"]
            channel.platform = c["platform"]
            channel.url = c["url"]
            channel.screenname = c["screenname"]
            channel.country = c["country"]
            channel.influencer = c["influencer"]
            channel.public = c["public"]
            channel.chat = c["chat"]
            channel.notes = c["notes"]

            session.flush()
            session.commit()

        row += 1

    session.commit()


def get_db_session():
    engine = create_engine(os.environ["DB"])

    session_generator = sessionmaker()
    session_generator.configure(bind=engine)
    session = session_generator()

    return session


def get_scraper_controller():
    engine = create_engine(os.environ["DB"])

    controller = ScraperController()
    controller.connect_to_db(engine)

    scrapers = [VkontakteScraper(),
        TelegramTelethonScraper(),
        GettrScraper(),
        BitchuteScraper(),
        RumbleScraper()]

    controller.register_scrapers(scrapers)

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

    controller = get_scraper_controller()
    controller.archive_unarchived_media()


def init_db():
    engine = create_engine(os.environ["DB"])
    mapper_registry.metadata.create_all(bind=engine)


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="DEBUG", catch=True)
    logger.add("logs/cisticola.log", level="TRACE", rotation="100 MB")

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

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
    elif args.command == "sync-channels":
        sync_channels(args)
    elif args.command == "scrape-channels":
        scrape_channels(args)
    elif args.command == "archive-media":
        archive_media(args)
    elif args.command == "channel-info":
        scrape_channel_info(args)
    else:
        logger.error(f"Unrecognized command {args.command}")
