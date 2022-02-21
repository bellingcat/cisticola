import cisticola.transformer
import cisticola.base
import json


class TwitterTransformer(cisticola.transformer.Transformer):
    """A Twitter specific ScraperResult, with a method ETL/transforming"""

    __version__ = "TwitterTransformer 0.0.1"

    def transform(self, data: cisticola.base.ScraperResult) -> cisticola.base.TransformedResult:
        raw = json.loads(data.raw_data)

        transformed = cisticola.base.TransformedResult(
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