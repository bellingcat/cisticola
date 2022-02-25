from typing import List
import cisticola.base
import cisticola.scraper.base
from sqlalchemy.orm import sessionmaker
from loguru import logger


class ScraperController:
    """Registers scrapers, uses them to generate ScraperResults. Synchronizes
    everything with database via ORM."""

    def __init__(self):
        self.scrapers = []
        self.session = None
        self.mapper_registry = None

    def register_scraper(self, scraper: cisticola.scraper.base.Scraper):
        self.scrapers.append(scraper)

    def scrape_channels(self, channels: List[cisticola.base.Channel]):
        if self.session is None:
            logger.error("No DB session")
            return

        for channel in channels:
            handled = False

            for scraper in self.scrapers:
                if scraper.can_handle(channel):
                    session = self.session()
                    handled = True
                    added = 0

                    # get most recent post
                    session = self.session()
                    rows = session.query(cisticola.base.ScraperResult).where(
                        cisticola.base.ScraperResult.channel == channel.id).order_by(
                        cisticola.base.ScraperResult.date.desc()).limit(1).all()

                    if len(rows) == 1:
                        since = rows[0]
                    else:
                        since = None

                    posts = scraper.get_posts(channel, since=since)

                    for post in posts:
                        session.add(post)
                        added += 1

                    session.commit()
                    logger.info(
                        f"{scraper} found {added} new posts from {channel}")
                    break

            if not handled:
                logger.warning(f"No handler found for Channel {channel}")

    def connect_to_db(self, engine):
        # create tables
        cisticola.base.mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.session.configure(bind=engine)


class ETLController:
    """This class will transform the raw_data tables into a format more conducive to analysis."""

    def __init__(self):
        pass
