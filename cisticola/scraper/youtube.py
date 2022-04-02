from datetime import datetime, timezone
import json
from typing import Generator
import tempfile
from pathlib import Path
import os

import yt_dlp
from loguru import logger

from cisticola.base import Channel, ScraperResult, RawChannelInfo
from cisticola.scraper import Scraper

class YoutubeScraper(Scraper):
    """An implementation of a Scraper for Youtube, using youtube-dl"""
    __version__ = "YoutubeScraper 0.0.1"

    @logger.catch
    def get_posts(self, channel: Channel, since: ScraperResult = None, archive_media: bool = True) -> Generator[ScraperResult, None, None]:

        content_type = 'video/mp4'

        if since is None:
            since_date = datetime.min
            start_date = None
        else:
            since_date = since.date
            start_date = since.date.strftime('%Y%m%d')

        with tempfile.TemporaryDirectory() as temp_dir:

            daterange = yt_dlp.utils.DateRange(start = start_date)

            ydl_opts = {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "merge_output_format": "mp4",
                "outtmpl": f"{temp_dir}/%(id)s.%(ext)s",
                "daterange" : daterange}

            ydl = yt_dlp.YoutubeDL(ydl_opts)

            try:
                meta = ydl.extract_info(
                    channel.url,
                    download=archive_media)
            except yt_dlp.utils.DownloadError as e:
                raise e
            else:
                videos = meta['entries']
                valid_videos = [video for video in videos if since_date < datetime.strptime(video['upload_date'], '%Y%m%d')]
                        
                for video in valid_videos:

                    url = video['webpage_url']

                    archived_urls = {url: None}
                    
                    video_id = video["id"]
                    video_ext = video["ext"]

                    if archive_media:
                    
                        key = f"{video_id}.{video_ext}"

                        with open(Path(temp_dir)/key, "rb") as f:
                            media_blob = f.read()

                        archived_url = self.archive_blob(media_blob, content_type, key)
                        archived_urls[url] = archived_url

                    yield ScraperResult(
                        scraper=self.__version__,
                        platform="Youtube",
                        channel=channel.id,
                        platform_id=video_id,
                        date=datetime.strptime(video['upload_date'], '%Y%m%d').replace(tzinfo=timezone.utc),
                        date_archived=datetime.now(timezone.utc),
                        raw_posts=json.dumps(video, default = str),
                        archived_urls=archived_urls,
                        media_archived=archive_media)
                        
    def can_handle(self, channel):
        if channel.platform == "Youtube" and channel.url:
            return True

    def archive_files(self, result: ScraperResult) -> ScraperResult:
        for url in result.archived_urls:
            if result.archived_urls[url] is None:

                media_blob = None

                with tempfile.TemporaryDirectory() as temp_dir:

                    ydl_opts = {
                        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                        "merge_output_format": "mp4",
                        "outtmpl": f"{temp_dir}/%(id)s.%(ext)s"}

                    ydl = yt_dlp.YoutubeDL(ydl_opts)

                    try:
                        ydl.download(url)
                    except yt_dlp.utils.DownloadError as e:
                        raise e
                        
                    files = os.listdir(temp_dir)
                    if len(files) != 1:
                        logger.warning(f'{len(files)} files downloaded for video: {url}')
                    key = files[0]
                    with open(Path(temp_dir, key), 'rb') as f:
                        media_blob = f.read()

                if media_blob is not None:
                    content_type = 'video/mp4'            
                    archived_url = self.archive_blob(media_blob, content_type, key)
                    result.archived_urls[url] = archived_url

        result.media_archived = True
        return result

    def get_profile(self, channel: Channel) -> RawChannelInfo:
        ydl_opts = {}
        ydl = yt_dlp.YoutubeDL(ydl_opts)

        meta = None
        try:
            meta = ydl.extract_info(
                channel.url,
                process=False)
            meta.pop('entries')

            return RawChannelInfo(scraper=self.__version__,
                platform=channel.platform,
                channel=channel.id,
                raw_data=json.dumps(meta),
                date_archived=datetime.now(timezone.utc))

        except yt_dlp.utils.DownloadError as e:
            raise e
