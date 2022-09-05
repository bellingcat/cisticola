from typing import Generator, Tuple, List
import os
from io import BytesIO
from urllib.parse import urlparse
import tempfile
from datetime import datetime, timezone

import boto3
from loguru import logger
import ffmpeg
from sqlalchemy.orm import sessionmaker
import yt_dlp
from sqlalchemy.sql.expression import func
from pathlib import Path
from sqlalchemy import nullsfirst

from cisticola.base import Channel, RawChannelInfo, ScraperResult, mapper_registry
from cisticola.utils import make_request

class Scraper:
    """Base class for defining platform-specific scrapers for scraping all posts 
    from a given channel on that specific platform. 
    """

    __version__ = "Scraper 0.0.0"

    cookiestring = os.environ["YOUTUBE_COOKIESTRING"].replace(r'\n', '\n').replace(r'\t', '\t')
    cookiefilename = 'cookiefile.txt'

    def __init__(self):

        # Initialize client to transfer files to the storage archive
        self.s3_client = boto3.client(
            service_name='s3',
            region_name=os.environ['DO_SPACES_REGION'],
            endpoint_url=f'https://{os.environ["DO_SPACES_REGION"]}.digitaloceanspaces.com',
            aws_access_key_id=os.environ['DO_SPACES_KEY'],
            aws_secret_access_key=os.environ['DO_SPACES_SECRET'])
        
        # Define request headers (necessary to bypass scraping protection 
        # for several platform scrapers)
        self.headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'}

    def __str__(self):
        return self.__version__

    def get_username_from_url(self, url: str) -> str:
        """Extract a channel's username from its URL. 

        Parameters
        ----------
        url: str
            URL of the channel on a given platform
            e.g. ``"https://twitter.com/EliotHiggins"``
        
        Returns
        -------
        username: str
            Extracted username of the channel.
            e.g. ``"EliotHiggins"``
        """
        
        raise NotImplementedError

    def url_to_key(self, url: str, content_type: str) -> str:
        """Generate a unique identifier for media from a specified post.

        Parameters
        ---------
        url: str
            URL of original post. 
            e.g. ``"https://twitter.com/bellingcat/status/1503397267675533313"``
        content_type: str
            Content-Type of media. 
            e.g. ``"image/jpeg"``

        Returns
        -------
        key: str
            Unique identifier for the media file from a specified post based on 
            the original post URL and the media's Content-Type. 
        """

        key = urlparse(url).path.split('/')[-1]
        return key 

    def url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:
        """Download media file from a specified media file URL.

        Parameters
        ---------
        url: str
            URL of media file from original post. 
            e.g. ``"https://pbs.twimg.com/media/FN0j0dYWUAcQxfK?format=png&name=medium"``
        key: str or None
            Pre-defined unique identifier for the media file.

        Returns
        -------
        blob: bytes
            Raw bytes of the downloaded media file. 
        content_type: str
            Content-Type of media. 
            e.g. ``"image/jpeg"``.
        key: str
            Unique identifier for the media file.
        """

        r = make_request(url, headers = self.headers)

        blob = r.content
        content_type = r.headers.get('Content-Type')

        if key is None:
            key = self.url_to_key(url, content_type)

        return blob, content_type, key

    def m3u8_url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:
        """Download media file from a specified media URL, where the media file 
        is formatted as an m3u8 playlist, which is then decoded to an mp4 file.

        Parameters
        ---------
        url: str
            URL of m3u8 playlist file from original post. 
            e.g. ``"https://media.gettr.com/group47/origin/2022/03/15/01/cbc436c1-1a1a-4b97-671d-c42109f3ec9b/out.m3u8"``
        key: str or None
            Pre-defined unique identifier for the media file.

        Returns
        -------
        blob: bytes
            Raw bytes of the downloaded media file. 
        content_type: str
            Content-Type of media. 
            e.g. ``"video/mp4"``.
        key: str
            Unique identifier for the media file.
        """
        
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

    def ytdlp_url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:
        """Download media file from a specified media URL, using a fork of 
        youtube-dl that enables faster downloading.

        Parameters
        ---------
        url: str
            URL of media file from original post. 
            e.g. ``"https://rumble.com/embed/vgt7gh/"``
        key: str or None
            Pre-defined unique identifier for the media file.

        Returns
        -------
        blob: bytes
            Raw bytes of the downloaded media file. 
        content_type: str
            Content-Type of media. 
            e.g. ``"video/mp4"``.
        key: str
            Unique identifier for the media file.
        """

        content_type = 'video/mp4'

        with tempfile.TemporaryDirectory() as temp_dir:
            cookiefile = Path(temp_dir)/self.cookiefilename
            with open(cookiefile, 'w') as f:
                f.write(self.cookiestring)
                
            ydl_opts = {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "merge_output_format": "mp4",
                "outtmpl": f"{temp_dir}/%(id)s.%(ext)s",
                "noplaylist": True,
                "quiet": True,
                "verbose": False,
                "retries": 5,
                "cookiefile": cookiefile}

            ydl = yt_dlp.YoutubeDL(ydl_opts)

            try:
                meta = ydl.extract_info(
                    url,
                    download=True,)
            except yt_dlp.utils.DownloadError as e:
                raise e
            else:
                video_id = meta["id"]
                video_ext = meta["ext"]
                
                with open(f"{temp_dir}/{video_id}.{video_ext}", "rb") as f:
                    blob = f.read()

        if key is None:
            key = self.url_to_key(url = url, content_type = content_type)

        return blob, content_type, key

    def archive_blob(self, blob: bytes, content_type: str, key: str) -> str:
        """Upload raw bytes of a media file to the storage archive. 

        Parameters
        ----------
        blob: bytes
            Raw bytes of the media file to be archived.
        content_type: str
            Content-Type of media. 
            e.g. ``"video/mp4"``.
        key: str
            Unique identifier for the media file.

        Returns
        -------
        archived_url: str
            URL specifying the file on the storage archive.
        """

        filename = self.__version__.replace(' ', '_') + '/' + key

        self.s3_client.upload_fileobj(BytesIO(blob), Bucket=os.environ[
            'DO_BUCKET'], Key=filename, ExtraArgs={'ACL': 'public-read', 'ContentType': content_type})

        archived_url = os.environ['DO_URL'] + '/' + filename

        return archived_url

    @logger.catch
    def archive_files(self, result: ScraperResult) -> ScraperResult:
        """Archive files corresponding to ``archived_url`` dict keys, if the 
        files have not previously been archived.

        Parameters
        ----------
        result: ScraperResult
            Previously scraped ScraperResult run with ``archive_media=False``.

        Returns
        -------
        ScraperResult
            Same ScraperResult as ``result``, but with all URLs in ``archived_url`` dict archived.
        """

        for url in result.archived_urls:
            if result.archived_urls[url] is None:
                media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_blob(media_blob, content_type, key)
                result.archived_urls[url] = archived_url

        result.media_archived = datetime.now(timezone.utc)
        return result

    def can_handle(self, channel: Channel) -> bool:
        """Whether or not the scraper can scrape the specified channel.

        Parameters
        ----------
        channel: Channel
            Channel to be scraped. 
        
        Returns
        -------
        bool
            ``True`` if the scraper is capable of scraping ``channel``,
            ``False`` if not. 
        """

        raise NotImplementedError

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        """Scrape all posts from the specified Channel.

        Parameters
        ----------
        channel: Channel
            Channel to be scraped.
        since: ScraperResult or None
            Most recently scraped ScraperResult from a previous scrape, or 
            ``None`` if scraper has not run before.
        archive_media: bool
            If ``True``, any media files (images, video, etc.) from posts are archived. 
            If ``False``, media files are not archived. 

        Yields
        ------
        ScraperResult
            Scraper result from a single post/comment from the specified Channel.
        """
        
        raise NotImplementedError


class ScraperController:
    """Registers scrapers, uses them to generate ScraperResults. Synchronizes
    everything with database via ORM."""

    def __init__(self):
        self.scrapers = []
        self.session = None

    def register_scraper(self, scraper: Scraper):
        """Register a single Scraper instance to the controller.
        """
        self.scrapers.append(scraper)

    def register_scrapers(self, scraper: List[Scraper]):
        """Register a list of Scraper instances to the controller.
        """
        self.scrapers.extend(scraper)

    def remove_all_scrapers(self):
        self.scrapers = []

    def scrape_all_channels(self, archive_media: bool = True, fetch_old: bool = False):
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        # TODO there should be a better/more generic way of selecting scrapeable channels
        channels = session.query(Channel).filter((Channel.source=='researcher')|(Channel.source=='snowball_it')).all()

        session.close()

        return self.scrape_channels(channels, archive_media=archive_media, fetch_old=fetch_old)

    def scrape_all_channel_info(self):
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        # Because of rate limiting, we may not be able to succesfully scrape info for all of these channels.
        # This will sort the channels by the least recently scraped.
        most_recently_archived = session.query(func.max(RawChannelInfo.date_archived).label("date"), RawChannelInfo.channel.label("channel")).group_by(RawChannelInfo.channel).subquery()
        channels = session.query(Channel).\
            filter((Channel.source=='researcher')|(Channel.source=='snowball_it')).\
            outerjoin(most_recently_archived, Channel.id == most_recently_archived.c.channel).\
            order_by(nullsfirst(most_recently_archived.c.date.asc())).all()

        session.close()
        return self.scrape_channel_info(channels)
    
    def scrape_channels(self, channels: List[Channel], archive_media: bool = True, fetch_old: bool = False):
        """Scrape all posts for all specified channels. 

        Parameters
        ----------
        channels: list<Channel>
            List of Channel instances to be scraped
        archive_media: bool
            If ``True``, any media files (images, video, etc.) from posts are archived. 
            If ``False``, media files are not archived. 
        """

        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        # If any channels are not already in the database, add them
        for channel in channels:

            platform_id = None
            if channel.platform_id not in (None, ''):
                platform_id = channel.platform_id

            channel_in_db = session.query(Channel).filter_by(platform_id=platform_id, platform=channel.platform, url=channel.url).first()

            if not channel_in_db:
                logger.debug(f"{channel} does not exist in database, adding")
                session.add(channel)
                session.flush()
                session.commit()

            handled = False

            for scraper in self.scrapers:
                if scraper.can_handle(channel):
                    logger.debug(f"{scraper} is handling {channel}")
                    handled = True
                    added = 0

                    if fetch_old and channel.platform == 'Telegram':
                        # get oldest post (currently only for Telegram)
                        # TODO fix this so that it doesn't have an explicit check on channel.platform (should be generic)
                        # TODO implement until on all scrapers
                        rows = session.query(ScraperResult).where(
                            ScraperResult.channel == channel.id).order_by(
                            ScraperResult.date.asc(), ScraperResult.id.desc()).limit(10).all()

                        if len(rows) > 0:
                            until = rows[0]
                        else:
                            until = None

                        posts = scraper.get_posts(channel, until=until, archive_media=archive_media)

                    else:
                        # get most recent post
                        # Note: a "bug" in Postgres can cause this query to hang for a really long time
                        # when searching for a single row, hence the limit(10).all() when we really just need
                        # the first row.
                        rows = session.query(ScraperResult).where(
                            ScraperResult.channel == channel.id).order_by(
                            ScraperResult.date.desc(), ScraperResult.id.asc()).limit(10).all()

                        if len(rows) > 0:
                            since = rows[0]
                        else:
                            since = None

                        posts = scraper.get_posts(channel, since=since, archive_media=archive_media)

                    for post in posts:
                        session.add(post)
                        session.commit()
                        added += 1

                    session.commit()
                    logger.info(
                        f"{scraper} found {added} new posts from {channel}")
                    break

            if not handled:
                logger.warning(f"No handler found for Channel {channel}")

        session.close()

    @logger.catch(reraise = True)
    def archive_unarchived_media(self, chronological=False):
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        while True:
            if chronological:
                posts = session.query(ScraperResult).where(ScraperResult.media_archived == None).where(ScraperResult.id >= 0).order_by(ScraperResult.date.desc()).limit(5000).all()
            else:
                # this query is really slow (~2.5 minutes) because of the shuffle. shuffling is so that multiple media archivers could work
                # simultaneously with low risk of collision (at least while the number of unarchived items is very large)
                posts = session.query(ScraperResult).where(ScraperResult.media_archived == None).order_by(func.random()).limit(5000).all()

            logger.info(f"Found {len(posts)} posts without media. Archiving now")

            for post in posts:
                handled = False

                for scraper in self.scrapers:
                    # compare major versions
                    if scraper.__version__.split('.')[0] == post.scraper.split('.')[0]:
                        handled = True
                        logger.debug(f"{scraper} is archiving media for ID {post.id}")
                        post = scraper.archive_files(post)

                        if post:
                            session.query(ScraperResult).where(ScraperResult.id == post.id).update({'archived_urls': post.archived_urls, 'media_archived': post.media_archived})
                            session.commit()

                        break
                
                if not handled:
                    logger.warning(f"No handler found for post scraped with {post.scraper}")

            session.commit()
            
        session.close()

    @logger.catch(reraise = True)
    def scrape_channel_info(self, channels: List[Channel]):
        """Scrape channel info for specified channels. 

        Parameters
        ----------
        channels: list<Channel>
            List of Channel instances to be scraped
        archive_media: bool
            If ``True``, any media files (images, video, etc.) from posts are archived. 
            If ``False``, media files are not archived. 
        """

        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()
        for channel in channels:
            handled = False

            for scraper in self.scrapers:
                if scraper.can_handle(channel):
                    logger.debug(f"{scraper} is getting channel info for {channel}")
                    handled = True

                    # get most recent post

                    try:
                        info = scraper.get_profile(channel)
                        if info is None:
                            logger.warning(f"No info returned for {channel}")
                            break

                        session.add(info)

                        session.commit()
                        logger.info(
                            f"{scraper} found {info}")
                        break
                    except ChannelDoesNotExistError:
                        logger.warning(f"ChannelDoesNotExist {channel}")

            if not handled:
                logger.warning(f"No handler found for Channel {channel}")

        session.close()

    def connect_to_db(self, engine):
        """Connect the specified SQLAlchemy engine to the controller.
        """
        
        # create tables
        mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.engine = engine
        self.session.configure(bind=self.engine)

    def reset_db(self):
        """Drop all data from the connected SQLAlchemy database.
        """

        mapper_registry.metadata.drop_all(bind=self.engine)
        self.connect_to_db(self.engine)

class ChannelDoesNotExistError(Exception):
    """The specified channel does not exist or has been deleted."""
