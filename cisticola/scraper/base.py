from typing import Generator, Tuple
import cisticola.base
import requests
import os
import boto3
from io import BytesIO
from urllib.parse import urlparse
from loguru import logger

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

    def archive_media(self, blob: bytes, content_type: str, key: str) -> str:

        filename = self.__version__.replace(' ', '_') + '/' + key

        self.s3_client.upload_fileobj(BytesIO(blob), Bucket=os.getenv(
            'DO_BUCKET'), Key=filename, ExtraArgs={'ACL': 'public-read', 'ContentType': content_type})

        archived_url = os.getenv('DO_URL') + '/' + filename

        return archived_url

    def can_handle(self, channel: cisticola.base.Channel) -> bool:
        pass

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        pass
