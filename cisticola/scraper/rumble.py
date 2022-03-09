from datetime import datetime
import json
from typing import Generator, Tuple
import tempfile
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import youtube_dl

from cisticola.base import Channel, ScraperResult
from cisticola.scraper import Scraper, make_request

BASE_URL = 'https://rumble.com'

class RumbleScraper(Scraper):
    """An implementation of a Scraper for Rumble, using custom functions"""
    __version__ = "RumbleScraper 0.0.1"

    def get_username_from_url(url):
        username = url.split('https://rumble.com/c/')[1]

        return username

    def get_posts(self, channel: Channel, since: ScraperResult = None, media: bool = True) -> Generator[ScraperResult, None, None]:

        username = RumbleScraper.get_username_from_url(channel.url)
        scraper = get_channel_videos(username)

        for post in scraper:
            if since is not None and datetime.fromtimestamp(post['cdate']*0.001) <= since.date:
                break

            archived_urls = {}

            if media:

                url = post['media_url']

                media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_media(media_blob, content_type, key)
                archived_urls[post['media_url']] = archived_url

            yield ScraperResult(
                scraper=self.__version__,
                platform="Rumble",
                channel=channel.id,
                platform_id=post['media_url'].split('/')[-2],
                date=datetime.fromisoformat(post['datetime']).replace(tzinfo=None),
                date_archived=datetime.now(),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Rumble" and RumbleScraper.get_username_from_url(channel.url) is not None:
            return True

    def url_to_blob(self, url: str, key: str = None) -> Tuple[bytes, str, str]:
        
        content_type = 'video/mp4'
        ext = '.' + content_type.split('/')[-1]

        with tempfile.TemporaryDirectory() as temp_dir:
            ydl_opts = {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "merge_output_format": "mp4",
                "outtmpl": f"{temp_dir}/%(id)s.%(ext)s",
                "noplaylist": True,
                'quiet': True,
                "verbose": False,}
            ydl = youtube_dl.YoutubeDL(ydl_opts)

            try:
                meta = ydl.extract_info(
                    url,
                    download=True,)
            except youtube_dl.utils.DownloadError as e:
                raise e
            else:
                video_id = meta["id"]
                video_ext = meta["ext"]
                
                with open(f"{temp_dir}/{video_id}.{video_ext}", "rb") as f:
                    blob = f.read()

        if key is None:
            key = urlparse(url).path.split('/')[-2] + ext

        return blob, content_type, key

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

def get_channel_videos(channel):
    
    page = 1
    channel_url = f'{BASE_URL}/c/{channel}?page='

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

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#