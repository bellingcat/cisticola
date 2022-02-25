from typing import Generator
import cisticola.base
import requests
import os
import boto3
from io import BytesIO
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

        pass

    def __str__(self):
        return self.__version__

    def archive_media(self, url: str, key: str = None) -> str:
        n_retries = 0
        r = requests.get(url)

        while r.status_code != 200 and n_retries < 5:
            logger.warning(f"{n_retries}/5: Request for {url} failed")
            n_retries += 1
            r = requests.get(url)

        if r.status_code != 200:
            logger.error(f"Could not fetch URL {url}")
            return url

        blob = r.content
        
        content_type = r.headers.get('Content-Type')

        if key is None:
            key = url.split('/')[-1]
            key = key.split('?')[0]

        filename = self.__version__.replace(' ', '_') + '/' + key

        self.s3_client.upload_fileobj(BytesIO(blob), Bucket=os.getenv(
            'DO_BUCKET'), Key=filename, ExtraArgs={'ACL': 'public-read', 'ContentType': content_type})

        archived_url = os.getenv('DO_URL') + '/' + filename

        return archived_url

    def can_handle(self, channel: cisticola.base.Channel) -> bool:
        pass

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:
        pass
