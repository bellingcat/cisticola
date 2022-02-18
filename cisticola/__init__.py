from typing import List
from datetime import datetime
from dataclasses import dataclass
import cisticola.scraper
import cisticola.base
from sqlalchemy.orm import sessionmaker


class ScraperController:
    """Registers scrapers, uses them to generate ScraperResults. Synchronizes
    everything with database via ORM."""

    def __init__(self):
        self.scrapers = []
        self.session = None
        self.mapper_registry = None

    def register_scraper(self, scraper: cisticola.base.Scraper):
        self.scrapers.append(scraper)

    def scrape_channels(self, channels: List[cisticola.base.Channel]):
        if self.session is None:
            cisticola.base.logger.error("No DB session")
            return

        for channel in channels:
            handled = False

            for scraper in self.scrapers:
                if scraper.can_handle(channel):
                    # get most recent post
                    session = self.session()
                    rows = session.query(cisticola.base.ScraperResult).order_by(
                        cisticola.base.ScraperResult.date_archived).limit(1).all()

                    if len(rows) == 1:
                        since = rows[0]
                    else:
                        since = None

                    posts = scraper.get_posts(channel, since=since)
                    handled = True

                    cisticola.base.logger.info(
                        f"{scraper} found {len(posts)} new posts from {channel}")
                    break

            if not handled:
                cisticola.base.logger.warning(
                    f"No handler found for Channel {channel}")

        session = self.session()
        session.bulk_save_objects(posts)
        session.commit()

        cisticola.base.logger.info(f"Added {len(posts)} entries to database")

    def connect_to_db(self, engine):
        # create tables
        cisticola.base.mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.session.configure(bind=engine)


class ETLController:
    """This class will transform the raw_data tables into a format more conducive to analysis."""

    def __init__(self):
        pass
