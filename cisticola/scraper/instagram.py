from typing import Generator
from datetime import datetime, timezone
import os
import json
import tempfile
from pathlib import Path

from loguru import logger
import instaloader 

from cisticola.base import Channel, ScraperResult
from cisticola.scraper.base import Scraper

BASE_URL = 'https://www.instagram.com/'

CONTENT_TYPES = {
    'jpg' : 'image/jpeg',
    'mp4' : 'video/mp4'}

class InstagramScraper(Scraper):
    __version__ = "InstagramScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split(BASE_URL)[1].strip('/')
        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)

        loader = instaloader.Instaloader(
            quiet = True,
            download_comments = False,
            save_metadata = False)
            
        loader.login(
            user = os.environ['INSTAGRAM_USERNAME'], 
            passwd = os.environ['INSTAGRAM_PASSWORD'])

        profile = instaloader.Profile.from_username(
            context = loader.context, 
            username = username)

        for post in profile.get_posts():

            if since is not None and post.date_utc <= since.date:
                break

            post_url = f'{BASE_URL}p/{post.shortcode}/'

            archived_urls = {}

            if archive_media:

                with tempfile.TemporaryDirectory() as temp_dir:

                    loader.download_post(post = post, target = Path(temp_dir))

                    files = os.listdir(temp_dir)
                    files = [f for f in files if not f.endswith('.txt')]

                    for file in files:
                        ext = file.split('.')[-1]
                        content_type = CONTENT_TYPES[ext]
                        filename = Path(temp_dir, file)
                        key = f'{post.shortcode}__{file}'
                    
                        with open(filename, 'rb') as f:
                            blob = f.read()
                
                        archived_url = self.archive_blob(blob = blob, content_type = content_type, key = key)
                        archived_urls[post_url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Instagram",
                channel=channel.id,
                platform_id=post.mediaid,
                date=post.date_utc,
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post._asdict(), default=str),
                archived_urls=archived_urls)

            for comment in post.get_comments():

                comment_dict = comment._asdict()
                comment_dict['post_url'] = post_url 
                comment_dict['is_comment'] = True

                yield ScraperResult(
                    scraper=self.__version__,
                    platform="Instagram",
                    channel=channel.id,
                    platform_id=post.mediaid,
                    date=comment.created_at_utc,
                    date_archived=datetime.now(timezone.utc),
                    raw_data=json.dumps(comment_dict, default=str),
                    archived_urls={})

    def can_handle(self, channel):
        if channel.platform == "Instagram" and self.get_username_from_url(channel.url) is not None:
            return True