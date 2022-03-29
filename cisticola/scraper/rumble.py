from datetime import datetime, timezone
import json
from typing import Generator
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from cisticola.base import Channel, ScraperResult
from cisticola.scraper import Scraper, make_request

BASE_URL = 'https://rumble.com'

class RumbleScraper(Scraper):
    """An implementation of a Scraper for Rumble, using custom functions"""
    __version__ = "RumbleScraper 0.0.1"

    def get_username_from_url(self, url):
        username = url.split('https://rumble.com/c/')[1]

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        username = self.get_username_from_url(channel.url)
        scraper = get_channel_videos(username)

        for post in scraper:
            if since is not None and datetime.fromtimestamp(post['cdate']*0.001) <= since.date:
                break

            archived_urls = {}

            if archive_media:

                url = post['media_url']

                media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                archived_url = self.archive_blob(media_blob, content_type, key)
                archived_urls[post['media_url']] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Rumble",
                channel=channel.id,
                platform_id=post['media_url'].split('/')[-2],
                date=datetime.fromisoformat(post['datetime']).replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def url_to_key(self, url: str, content_type: str) -> str:
        ext = '.' + content_type.split('/')[-1]
        key = urlparse(url).path.split('/')[-2] + ext
        return key 

    def can_handle(self, channel):
        if channel.platform == "Rumble" and self.get_username_from_url(channel.url) is not None:
            return True

    def get_profile(self, channel: Channel) -> dict:

        username = self.get_username_from_url(channel.url)
        profile = get_channel_profile(username = username)

        return profile

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

def get_media_url(url):
    
    r = make_request(url = url)
    soup = BeautifulSoup(r.content, features = 'lxml')
    
    script = json.loads(''.join(soup.find('script', {'type':'application/ld+json'}).text))
    media_url = script[0]['embedUrl']
    
    return media_url

def process_video(video):
    
    rumble_soup = video.find('span', {'class' : 'video-item--rumbles'})
    if rumble_soup is None:
        rumbles = '0'
    else:
        rumbles = rumble_soup['data-value']

    info = {
        'title' : video.find('h3').text,
        'thumbnail' : video.find('img')['src'],
        'link' : BASE_URL + video.find('a', href = True)['href'],
        'views' : video.find('span', {'class' : 'video-item--views'})['data-value'],
        'rumbles' : rumbles,
        'duration' : video.find('span', {'class' : 'video-item--duration'})['data-value'],
        'datetime' : video.find('time')['datetime']}
    
    info['media_url'] = get_media_url(info['link'])
    
    return info

def get_channel_videos(username):
    
    page = 1
    channel_url = f'{BASE_URL}/c/{username}?page='

    while True:
        url = channel_url + str(page)
        r = make_request(url = url, break_codes = [404])

        if r.status_code == 404:
            break

        soup = BeautifulSoup(r.content, features = 'lxml')

        video_list = soup.find_all('li', {'class' : 'video-listing-entry'})

        for video in video_list:
            yield process_video(video)

        page += 1

def get_channel_profile(username):

    channel_url = f'{BASE_URL}/c/{username}'
    r = make_request(url = channel_url)
    soup = BeautifulSoup(r.content, features = 'lxml')

    verified_svg = soup.find('h1').find('svg', {'class' : 'listing-header--verified'})

    profile = {
        'name': soup.find('h1').text,
        'verified': verified_svg is not None,
        'thumbnail': soup.find('img', {'class' : 'listing-header--thumb'})['src'],
        'cover':  soup.find('img', {'class' : 'listing-header--backsplash-img'})['src'],
        'subscribers': soup.find('span', {'class' : 'subscribe-button-count'}).text}

    return profile

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#