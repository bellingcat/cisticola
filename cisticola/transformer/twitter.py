import json
from loguru import logger
from typing import Generator, Union, Callable
import dateutil.parser

from cisticola.transformer.base import Transformer 
from cisticola.base import ScraperResult, TransformedResult, Image, Video, Media, Channel

class TwitterTransformer(Transformer):
    """A Twitter specific ScraperResult, with a method ETL/transforming"""

    __version__ = "TwitterTransformer 0.0.1"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(' ')
        if scraper[0] == "TwitterScraper":
            return True

        return False        

    def process_media(self, tweet, post_id, data):
        if tweet['media']:
            for media in tweet['media']:
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
                        m = Image(url=new, post=post_id, raw_id=data.id, original_url=orig)
                    else:
                        m = Video(url=new, post=post_id, raw_id=data.id, original_url=orig)

                    yield m


    def transform(self, data: ScraperResult, insert: Callable) -> Generator[Union[TransformedResult, Channel, Media], None, None]:
        raw = json.loads(data.raw_data)

        transformed = TransformedResult(
            raw_id=data.id,
            platform_id=raw['id'],
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=dateutil.parser.parse(raw['date']),
            date_archived=data.date_archived,
            url=raw['url'],
            content=raw['content'],
            author_id=raw['user']['id'],
            author_username=raw['user']['username'])

        def subtweet(tweet):
            channel = Channel(
                name=tweet['user']['displayname'],
                platform_id=tweet['user']['id'],
                platform=data.platform,
                url=tweet['user']['url'],
                screenname=tweet['user']['username'],
                category='forwarded',
                source=self.__version__
                )

            channel = insert(channel)

            original = TransformedResult(
                raw_id=data.id,
                platform_id=tweet['id'],
                scraper=data.scraper,
                transformer=self.__version__,
                platform=data.platform,
                channel=channel.id,
                date=dateutil.parser.parse(tweet['date']),
                date_archived=data.date_archived,
                url=tweet['url'],
                content=tweet['content'],
                author_id=tweet['user']['id'],
                author_username=tweet['user']['username']
            )

            original = insert(original)
            transformed.forwarded_from = channel.id
            transformed.reply_to = original.id

            media = self.process_media(tweet, original.id, data)
            for m in media:
                insert(m)

        if raw['retweetedTweet'] is not None:
            subtweet(raw['retweetedTweet'])

        if raw['quotedTweet'] is not None:
            subtweet(raw['quotedTweet'])

        insert(transformed)

        media = self.process_media(raw, transformed.id, data)
        for m in media:
            insert(m)


        
