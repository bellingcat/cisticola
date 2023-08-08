import json
from datetime import datetime, timezone
from typing import Callable

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from cisticola.base import ChannelInfo, Post, RawChannelInfo, ScraperResult, Video
from cisticola.transformer.base import Transformer


class BitchuteTransformer(Transformer):
    """A Bitchute specific ScraperResult, with a method ETL/transforming"""

    __version__ = "BitchuteTransformer 0.0.2"

    def can_handle(self, data: ScraperResult) -> bool:
        scraper = data.scraper.split(" ")
        if scraper[0] == "BitchuteScraper":
            return True

        return False

    def transform_media(self, data: ScraperResult, transformed: Post, insert: Callable):
        raw = json.loads(data.raw_data)

        orig = raw["video_url"]
        new = data.archived_urls[orig]

        m = Video(
            url=new,
            post=transformed.id,
            raw_id=data.id,
            original_url=orig,
            date=data.date,
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            transformer=self.__version__,
            scraper=data.scraper,
            platform=data.platform,
        )

        insert(m)

    def transform_info(
        self, data: RawChannelInfo, insert: Callable, session, channel=None
    ):
        raw = json.loads(data.raw_data)

        transformed = ChannelInfo(
            raw_channel_info_id=data.id,
            channel=data.channel,
            platform_id=raw["owner_url"].strip("/").split("/")[-1],
            platform=data.platform,
            scraper=data.scraper,
            transformer=self.__version__,
            screenname=raw["owner_name"],
            name=raw["owner_name"],
            description=raw["description"],
            description_url="",  # does not exist for Bitchute
            description_location="",  # does not exist for Bitchute
            followers=raw["subscribers"],
            following=-1,  # does not exist for Bitchute
            verified=False,  # does not exist for Bitchute
            date_created=parse_created(raw["created"], data.date_archived),
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
        )

        transformed = insert(transformed)

    def transform(
        self,
        data: ScraperResult,
        insert: Callable,
        session: Session,
        flush_posts: Callable,
    ):
        raw = json.loads(data.raw_data)

        if raw["category"] == "comment":
            if raw["parent_id"] is None:
                reply_to_id = raw["thread_id"]
            else:
                reply_to_id = raw["parent_id"]
            flush_posts()
            post = (
                session.query(Post)
                .filter_by(channel=data.channel, platform_id=reply_to_id)
                .first()
            )
            if post is None:
                if raw["parent_id"] is not None:
                    # this block is for comments whose parent_ids correspond to deleted comments
                    post = (
                        session.query(Post)
                        .filter_by(channel=data.channel, platform_id=raw["thread_id"])
                        .first()
                    )
                    if post is None:
                        reply_to = -1
                    else:
                        reply_to = post.id
                else:
                    reply_to = -1
            else:
                reply_to = post.id
            content = raw["body"].strip()
        else:
            reply_to = -1
            soup = BeautifulSoup(raw["body"], features="html.parser")
            soup.find("div", {"class": "teaser"}).decompose()
            soup.find("span", {"class": "more"}).decompose()
            soup.find("span", {"class": "less hidden"}).decompose()
            content = soup.text.strip()

        transformed = Post(
            raw_id=data.id,
            platform_id=raw["id"],
            scraper=data.scraper,
            transformer=self.__version__,
            platform=data.platform,
            channel=data.channel,
            date=data.date,
            date_archived=data.date_archived,
            date_transformed=datetime.now(timezone.utc),
            url=raw["url"] if raw["url"] else None,
            content=content,
            author_id=raw["author_id"],
            author_username=raw["author"],
            reply_to=reply_to,
            hashtags=list(
                filter(None, [h.strip("#") for h in raw["hashtags"].split(",")])
            ),
            likes=raw["likes"],
            views=int(raw["views"]) if raw.get("views") else None,
            video_title=raw["subject"],
            video_duration=_parse_duration_str(raw["length"]),
        )

        transformed = insert(transformed)


def parse_created(created: str, date_archived: datetime) -> datetime:
    """Convert a created string (e.g. ``"1 year, 10 months ago"``) to a datetime
    object relative to the specified ``date_archived``.
    """
    try:
        # handle case where `created` string has already been parsed into a datetime
        return datetime.fromisoformat(created)
    except ValueError:
        period_list = ["year", "month", "week", "day"]

        periods = [
            period.strip() for period in created.split("ago")[0].strip().split(",")
        ]
        _kwargs = {
            period: int(number)
            for period, number in dict(reversed(p.split(" ")) for p in periods).items()
        }
        kwargs = {(k + "s" if k in period_list else k): v for k, v in _kwargs.items()}

        return date_archived - relativedelta(**kwargs)


def _parse_duration_str(duration_str: str) -> int:
    """Convert duration string (e.g. '2:27:04') to the number of seconds (e.g. 8824)."""
    if not duration_str:
        return None
    else:
        duration_list = duration_str.split(":")
        return sum(
            [int(s) * int(g) for s, g in zip([1, 60, 3600], reversed(duration_list))]
        )
