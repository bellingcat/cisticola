from datetime import datetime, timezone
import json
from typing import Generator
from urllib.parse import urlparse
from loguru import logger

from bs4 import BeautifulSoup
import os

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper import Scraper, make_request

BASE_URL = 'https://rumble.com'

class RumbleScraper(Scraper):
    """An implementation of a Scraper for Rumble, using custom functions"""
    __version__ = "RumbleScraper 0.0.2"

    cookiestring = os.environ["YOUTUBE_COOKIESTRING"].replace(r'\n', '\n').replace(r'\t', '\t')
    cookiefilename = 'cookiefile.txt'

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        scraper = get_channel_videos(channel.url)

        for post in scraper:
            if since is not None and post['datetime'].replace(tzinfo=timezone.utc) <= since.date.replace(tzinfo=timezone.utc):
                break

            url = post['media_url']

            archived_urls = {url: None}

            if archive_media:

                media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                archived_url = self.archive_blob(media_blob, content_type, key)
                archived_urls[url] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Rumble",
                channel=channel.id,
                platform_id=post['media_url'].split('/')[-2],
                date=post['datetime'].replace(tzinfo=timezone.utc),
                date_archived=datetime.now(timezone.utc),
                raw_data=json.dumps(post, default = str),
                archived_urls=archived_urls,
                media_archived=datetime.now(timezone.utc) if archive_media else None)

    def url_to_key(self, url: str, content_type: str) -> str:
        ext = '.' + content_type.split('/')[-1]
        key = urlparse(url).path.split('/')[-2] + ext
        return key 

    @logger.catch
    def archive_files(self, result: ScraperResult) -> ScraperResult:
        for url in result.archived_urls:
            if result.archived_urls[url] is None:
                media_blob, content_type, key = self.ytdlp_url_to_blob(url)
                archived_url = self.archive_blob(media_blob, content_type, key)
                result.archived_urls[url] = archived_url

        result.media_archived = datetime.now(timezone.utc)
        return result

    def can_handle(self, channel):
        if channel.platform == "Rumble" and channel.url is not None:
            return True

    @logger.catch
    def get_profile(self, channel: Channel) -> RawChannelInfo:

        profile = get_channel_profile(url = channel.url)

        return RawChannelInfo(scraper=self.__version__,
            platform=channel.platform,
            channel=channel.id,
            raw_data=json.dumps(profile),
            date_archived=datetime.now(timezone.utc))

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

def get_media_url(url):
    
    r = make_request(url = url)
    soup = BeautifulSoup(r.content, features = 'html.parser')
    
    script = json.loads(''.join(soup.find('script', {'type':'application/ld+json'}).text))
    media_url = script[0]['embedUrl']
    
    return media_url

def process_video(video):
    
    rumble_soup = video.find('span', {'class' : 'video-item--rumbles'})
    if rumble_soup is None:
        rumbles = '0'
    else:
        rumbles = rumble_soup['data-value']

    view_span = video.find('span', {'class' : 'video-item--views'})
    if view_span is None:
        views = None
    else:
        views = view_span.get('data-value')
        
    author_a = video.find('a', {'rel': 'author'})
    if author_a is None:
        author_id = None
        author_name = None
    else:
        author_id = author_a['href'].split('/')[-1]
        author_name = author_a.text
    
    video_link = BASE_URL + video.find('a', href = True)['href']
    r = make_request(url = video_link)
    soup = BeautifulSoup(r.content, features = 'html.parser')
    
    content_div = soup.find('div', {'class': 'container content media-description'})
    
    info = {
        'title' : video.find('h3').text,
        'thumbnail' : video.find('img')['src'],
        'link' : video_link,
        'views' : views,
        'rumbles' : rumbles,
        'content': '' if content_div is None else content_div.get_text('\n'),
        'duration' : video.find('span', {'class' : 'video-item--duration'})['data-value'],
        'datetime' : datetime.fromisoformat(video.find('time')['datetime']),
        'author_id': author_id,
        'author_name': author_name}
    
    info['media_url'] = get_media_url(info['link'])
    
    return info


def get_channel_videos(url):
    
    page = 1
    channel_url = f'{url}?page='

    while True:
        url = channel_url + str(page)
        r = make_request(url = url, break_codes = [404])

        if r.status_code == 404:
            break

        soup = BeautifulSoup(r.content, features = 'html.parser')

        video_list = soup.find_all('li', {'class' : 'video-listing-entry'})

        for video in video_list:
            yield process_video(video)

        page += 1

def get_channel_profile(url):

    channel_url = f'{url}'
    r = make_request(url = channel_url)
    soup = BeautifulSoup(r.content, features = 'lxml')

    verified_svg = soup.find('h1').find('svg', {'class' : 'listing-header--verified'})
    thumbnail_soup = soup.find('img', {'class' : 'listing-header--thumb'})
    cover_soup = soup.find('img', {'class' : 'listing-header--backsplash-img'})

    author_a = soup.find('a', {'rel': 'author'})
    if author_a is None:
        author_id = None
    else:
        author_id = author_a['href'].split('/')[-1]

    profile = {
        'name': soup.find('h1').text,
        'id': author_id,
        'verified': verified_svg is not None,
        'thumbnail': thumbnail_soup.get('src') if thumbnail_soup else None,
        'cover':  cover_soup.get('src') if cover_soup else None,
        'subscribers': soup.find('span', {'class' : 'subscribe-button-count'}).text}
        
    return profile

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#