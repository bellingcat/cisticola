from typing import Generator, List
from datetime import datetime, timezone
import os
import json
import tempfile
from pathlib import Path

from loguru import logger
import instaloader 

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

BASE_URL = 'https://www.instagram.com/'

CONTENT_TYPES = {
    'jpg' : 'image/jpeg',
    'mp4' : 'video/mp4'}

class InstagramScraper(Scraper):
    """An implementation of a Scraper for Instagram, using instaloader library"""
    __version__ = "InstagramScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split(BASE_URL)[1].strip('/')
        return username

    @logger.catch
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

            archived_urls = get_archived_urls_from_post(post = post)

            for url in archived_urls.keys():

                if archive_media:
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Instagram",
                channel=channel.id,
                platform_id=post.mediaid,
                date=post.date_utc,
                date_archived=datetime.now(timezone.utc),
                raw_posts=json.dumps(post._asdict(), default=str),
                archived_urls=archived_urls,
                media_archived=archive_media)

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
                    raw_posts=json.dumps(comment_dict, default=str),
                    archived_urls={},
                    media_archived=True)

    def can_handle(self, channel):
        if channel.platform == "Instagram" and self.get_username_from_url(channel.url) is not None:
            return True

    def get_profile(self, channel: Channel) -> RawChannelInfo:

        username = self.get_username_from_url(channel.url)

        loader = instaloader.Instaloader(
            quiet = True,
            download_comments = False,
            save_metadata = False)

        loader.login(
            user = os.environ['INSTAGRAM_USERNAME'], 
            passwd = os.environ['INSTAGRAM_PASSWORD'])

        user_profile = instaloader.Profile.from_username(
            context = loader.context, 
            username = username)
        
        profile = user_profile._asdict()
        profile['followers'] = user_profile.followers
        profile['followees'] = user_profile.followees

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))

def get_archived_urls_from_post(post: instaloader.Post) -> List[str]:
    typename = post._node['__typename']
    if typename == 'GraphImage':
        urls = [post._node['display_url']]
    elif typename == 'GraphVideo':
        urls = [post._node['video_url']]
    elif typename == 'GraphSidecar':
        urls = [edge['node']['display_url'] for edge in post._node['edge_sidecar_to_children']['edges']]
    else:
        raise NotImplementedError(f'post of type {typename} is currently not supported.')
        
    return {url : None for url in urls}