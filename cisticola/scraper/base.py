from typing import Generator, Tuple, List
import os
from io import BytesIO
from urllib.parse import urlparse
import tempfile

import requests
import boto3
from loguru import logger
import ffmpeg
from sqlalchemy.orm import sessionmaker

from cisticola.base import Channel, ScraperResult, mapper_registry

class Scraper:
    __version__ = "Scraper 0.0.0"

    def __init__(self):
        self.s3_client = boto3.client('s3',
                                      region_name=os.getenv(
                                          'DO_SPACES_REGION'),
                                      endpoint_url='https://{}.digitaloceanspaces.com'.format(
                                          os.getenv('DO_SPACES_REGION')),
                                      aws_access_key_id=os.getenv(
                                          'DO_SPACES_KEY'),
                                      aws_secret_access_key=os.getenv('DO_SPACES_SECRET'))

        self.headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

        pass

    def __str__(self):
        return self.__version__

    def url_to_key(self, url: str, content_type: str) -> str:
        key = urlparse(url).path.split('/')[-1]
        return key 

    def url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:

        n_retries = 0

        r = requests.get(url, headers = self.headers)

        while r.status_code != 200 and n_retries < 5:
            logger.warning(f"{n_retries}/5: Request for {url} failed")
            n_retries += 1
            r = requests.get(url, headers = self.headers)

        if r.status_code != 200:
            logger.error(f"Could not fetch URL {url}")
            return url

        blob = r.content
        content_type = r.headers.get('Content-Type')

        if key is None:
            key = self.url_to_key(url, content_type)

        return blob, content_type, key

    def m3u8_url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:
        
        content_type = 'video/mp4'
        ext = '.' + content_type.split('/')[-1]

        with tempfile.NamedTemporaryFile(suffix = ext) as temp_file:
            
            (
                ffmpeg
                .input(url)
                .output(temp_file.name, vcodec='copy')
                .global_args('-loglevel', 'error')
                .run(overwrite_output=True))
            
            temp_file.seek(0)
            blob = temp_file.read()

        if key is None:
            key = self.url_to_key(url = url, content_type = content_type)

        return blob, content_type, key

    def archive_media(self, blob: bytes, content_type: str, key: str) -> str:

        filename = self.__version__.replace(' ', '_') + '/' + key

        self.s3_client.upload_fileobj(BytesIO(blob), Bucket=os.getenv(
            'DO_BUCKET'), Key=filename, ExtraArgs={'ACL': 'public-read', 'ContentType': content_type})

        archived_url = os.getenv('DO_URL') + '/' + filename

        return archived_url

    def can_handle(self, channel: Channel) -> bool:
        pass

    def get_posts(self, channel: Channel, since: ScraperResult = None) -> Generator[ScraperResult, None, None]:
        pass


class ScraperController:
    """Registers scrapers, uses them to generate ScraperResults. Synchronizes
    everything with database via ORM."""

    def __init__(self):
        self.scrapers = []
        self.session = None
        self.mapper_registry = None

    def register_scraper(self, scraper: Scraper):
        self.scrapers.append(scraper)

    def register_scrapers(self, scraper: List[Scraper]):
        self.scrapers.extend(scraper)

    def scrape_channels(self, channels: List[Channel]):
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
                    rows = session.query(ScraperResult).where(
                        ScraperResult.channel == channel.id).order_by(
                        ScraperResult.date.desc()).limit(1).all()

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
        mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.session.configure(bind=engine)


class ETLController:
    """This class will transform the raw_data tables into a format more conducive to analysis."""

    def __init__(self):
        pass
