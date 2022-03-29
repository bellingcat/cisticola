import argparse
from loguru import logger
import gspread
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cisticola.base import Channel
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

def sync_channels(args):
    logger.info("Synchronizing channels")

    session = get_db_session(args)

    gc = gspread.service_account(filename='service_account.json')

    # Open a sheet from a spreadsheet in one go
    wks = gc.open_by_url(args.gsheet).worksheet("channels")
    channels = wks.get_all_records()
    row = 2

    for c in channels:
        del c['id']
        del c['followers']

        for k in c.keys():
            if c[k] == 'TRUE' or c[k] == 'yes': c[k] = True
            if c[k] == 'FALSE' or c[k] == 'no': c[k] = False

        if c['public'] == '': c['public'] = False
        if c['chat'] == '': c['chat'] = False

        # check to see if this already exists, 
        channel = session.query(Channel).filter_by(platform_id=c['platform_id'], platform=c['platform'], url=c['url']).first()
        
        if not channel:
            channel = Channel(**c, source='researcher')
            session.add(channel)
            session.flush()
            wks.update_cell(row, 1, channel.id)

        row += 1

    session.commit()

def get_db_session(args):
    engine = create_engine(args.db)
    
    session_generator = sessionmaker()
    session_generator.configure(bind=engine)
    session = session_generator()

    return session

def get_scraper_controller(args):
    engine = create_engine(args.db)

    controller = ScraperController()
    controller.connect_to_db(engine)

    scrapers = [
        TelegramTelethonScraper(),
        TwitterScraper()]

    controller.register_scrapers(scrapers)

    return controller

def scrape_channels(args):
    logger.info(f"Scraping channels, media: {args.media}")

    controller = get_scraper_controller(args)
    controller.scrape_all_channels(archive_media = args.media)

def archive_media(args):
    logger.info(f"Archiving unarchived media")

    controller = get_scraper_controller(args)
    controller.archive_unarchived_media()

if __name__ == '__main__':
    logger.add("./test.log", level="TRACE")

    parser = argparse.ArgumentParser(description = 'Cisticola command line tools')
    parser.add_argument('command',  type=str, help='Command to run: "sync-channels", "scrape-channels", or "archive-media"')
    parser.add_argument('--db', type=str, help='[sync-channels, scrape-channels, archive-media] Sqlalchemy database string, eg, "sqlite:///cisticola.db"')
    parser.add_argument('--gsheet', type=str, help='[sync-channels] URL of Google Sheet to synchronize')
    parser.add_argument('--media', action='store_true', help='[scrape-channels] Scrapes media')

    args = parser.parse_args()

    if args.command == 'sync-channels':
        sync_channels(args)
    elif args.command == 'scrape-channels':
        scrape_channels(args)
    elif args.command == 'archive-media':
        archive_media(args)
    else:
        logger.error(f"Unrecognized command {args.command}")
