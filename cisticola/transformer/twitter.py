import json
from loguru import logger
from typing import Generator

from cisticola.transformer.base import Transformer 
from cisticola.base import ScraperResult, TransformedResult, Image, Video, Media

class TwitterTransformer(Transformer):
    """A Twitter specific ScraperResult, with a method ETL/transforming"""

    __version__ = "TwitterTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "TwitterScraper":
            return True

        return False        

    def transform_media(self, data: ScraperResult, transformed: TransformedResult) -> Generator[Media, None, None]:
        raw = json.loads(data.raw_data)

        if raw['media']:
            for media in raw['media']:
                orig = None

                if media["_type"] == "snscrape.modules.twitter.Photo":
                    orig = media["fullUrl"]
                elif media["_type"] == "snscrape.modules.twitter.Gif":
                    orig = media["variants"][0]["url"]
                elif media["_type"] == "snscrape.modules.twitter.Video":
                    variant = max([v for v in media["variants"] if v["bitrate"]], key=lambda v: v["bitrate"])
                    orig = variant["url"]
                
                if orig is None:
                    logger.warning(f"No media URL found for {media}")
                elif orig not in data.archived_urls:
                    logger.info("Media discovered but not archived")
                else:
                    new = data.archived_urls[orig]

                    if media["_type"] == "snscrape.modules.twitter.Photo":
                        m = Image(url=new, post=transformed.id, raw_id=data.id, original_url=orig)
                    else:
                        m = Video(url=new, post=transformed.id, raw_id=data.id, original_url=orig)

                    yield m

    def transform(self, data: ScraperResult) -> TransformedResult:
        raw = json.loads(data.raw_data)

        transformed = TransformedResult(
            raw_id=data.id,
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=data.date,
            date_archived=data.date_archived,
            url=raw['url'],
            content=raw['content'],
            author_id=raw['user']['id'],
            author_username=raw['user']['username'])

        return transformed
