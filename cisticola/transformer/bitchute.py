import json
from loguru import logger
from typing import Generator

from bs4 import BeautifulSoup 

from cisticola.transformer.base import Transformer 
from cisticola.base import ScraperResult, Post, Image, Video, Media

class BitchuteTransformer(Transformer):
    """A Bitchute specific ScraperResult, with a method ETL/transforming"""

    __version__ = "BitchuteTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "BitchuteScraper":
            return True

        return False        

    def transform_media(self, data: ScraperResult, transformed: Post) -> Generator[Media, None, None]:
        raw = json.loads(data.raw_posts)

        orig = raw['video_url']
        new = data.archived_urls[orig]

        m = Video(url=new, post=transformed.id, raw_id=data.id, original_url=orig)

        yield m

    def transform(self, data: ScraperResult) -> Post:
        raw = json.loads(data.raw_posts)

        soup = BeautifulSoup(raw['body'], features = 'html.parser')
        content = soup.find_all('p')[-1].text

        transformed = Post(
            raw_id=data.id,
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=data.date,
            date_archived=data.date_archived,
            url=raw['url'],
            content=content,
            author_id=raw['author_id'],
            author_username=raw['author'])

        return transformed
