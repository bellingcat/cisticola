@dataclass
class TransformedResult:
    """An object with fields for columns in the analysis table"""
    scraper: str
    transformer: str
    platform: str
    channel: str
    date: datetime
    date_archived: datetime
    url: str
    content: str
    author_id: str
    author_username: str


class TwitterResult(ScraperResult):
    """A Twitter specific ScraperResult, with a method ETL/transforming"""

    def transform(self) -> TransformedResult:
        data = json.loads(self.raw_data)

        transformed = TransformedResult(
            scraper=self.scraper,
            transformer=self.__version__,
            platform=self.platform,
            channel=self.channel,
            date=self.date,
            date_archived=self.date_archived,
            url=data['url'],
            content=data['content'],
            author_id=data['user']['id'],
            author_username=data['user']['username'])

        return transformed
