from datetime import datetime, timezone, date
import json
from typing import Generator
import os 
from loguru import logger

from gabber.client import Client, GAB_API_BASE_URL

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper.base import Scraper

class GabScraper(Scraper):
    """An implementation of a Scraper for Gab, using gabber library"""
    __version__ = "GabScraper 0.0.2"

    def get_username_from_url(self, url):
        username = url.split('https://gab.com/')[-1]

        return username

    def get_group_id_from_url(self, url):
        group_id = int(url.split('/')[-1])

        return group_id

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:
        client = Client(
            username = os.environ['GAB_USER'],
            password = os.environ['GAB_PASS'],
            threads = 25)

        if channel.url.split('/')[-2] == 'groups':

            group_id = self.get_group_id_from_url(url = channel.url)
            scraper = client.pull_group_posts(
                id = group_id,
                depth = float('inf')) 
        else:

            username = self.get_username_from_url(channel.url)

            result = client._get(GAB_API_BASE_URL + f"/account_by_username/{username}").json()
            user_id = int(result['id'])

            scraper = client.pull_statuses(
                id = user_id,
                created_after = date.min,
                replies = False)

        for post in scraper:
            if since is not None and datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                break

            media_urls = []
            archived_urls = {}

            if archive_media:

                for attachment in post.get('media_attachments'):
                    if attachment.get('type') == 'video':
                        media_urls.append(attachment['source_mp4'])
                    else:
                        media_urls.append(attachment['url'])
                        
                if post.get('reblog') is not None:
                    for attachment in post['reblog'].get('media_attachments'):
                        if attachment.get('type') == 'video':
                            media_urls.append(attachment['source_mp4'])
                        else:
                            media_urls.append(attachment['url'])

                for url in media_urls:
                    media_blob, content_type, key = self.url_to_blob(url)
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Gab",
                channel=channel.id,
                platform_id=post['id'],
                date=datetime.fromisoformat(post['created_at'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_posts=json.dumps(post),
                archived_urls=archived_urls,
                media_archived=archive_media)

    def can_handle(self, channel: Channel) -> bool:
        if channel.platform == "Gab" and self.get_username_from_url(channel.url) is not None:
            return True

    def get_profile(self, channel: Channel) -> RawChannelInfo:

        client = Client(
            username = os.environ['GAB_USER'],
            password = os.environ['GAB_PASS'],
            threads = 25)

        if channel.url.split('/')[-2] == 'groups':

            group_id = self.get_group_id_from_url(url = channel.url)
            profile = client.pull_group(id = group_id)
        
        else:

            username = self.get_username_from_url(channel.url)

            profile = client._get(GAB_API_BASE_URL + f"/account_by_username/{username}").json()

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))
