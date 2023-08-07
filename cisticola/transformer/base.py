from typing import List, Generator, Union, Callable
from loguru import logger
from sqlalchemy import cast, String
from sqlalchemy.orm import sessionmaker, make_transient
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.expression import func
from collections import defaultdict
from datetime import datetime, timezone
import spacy

from cisticola.base import (
    RawChannelInfo,
    ChannelInfo,
    ScraperResult,
    Post,
    Media,
    Channel,
    mapper_registry,
    Image,
    Video,
    Audio,
)


class Transformer:
    """Interface class for transformers."""

    __version__ = "Transformer 0.0.0"

    def __init__(self):
        pass

    def can_handle(data: ScraperResult) -> bool:
        """Specifies whether or not a Transformer is capable of handling a particular
        piece of scraped data.

        Parameters
        ----------
        data : ScraperResult
            The ScraperResult object to check for ability to handle.

        Returns
        -------
        bool
            ``True`` if it can be handled by this Transformer, false otherwise.
        """

        pass

    def transform(
        data: ScraperResult, insert: Callable
    ) -> Generator[Union[Post, Channel, Media], None, None]:
        """Transform a ScraperResult into objects with additional parameters for analysis. This function can
        yield multiple objects, as it will find references to quoted/replied posts, media objects, and Channel
        objects and provide all of these to be inserted into the database.

        Parameters
        ----------
        data : ScraperResult
            The ScraperResult object to process.
        insert : Callable
            A function that either inserts the object into a database or finds an object with the
            relevant unique constraints if applicable.
        """

        pass

    def transform_media(self, data: ScraperResult, transformed: Post, insert: Callable):
        """Transform a post's media attachment to standard form and insert into database.

        Parameters
        ----------
        data: cisticola.base.ScraperResult
            Raw post data of post that media file was attached to
        transformed: cisticola.base.Post
            Transformed post data of post that media file was attached to
        insert: Callable
            A function that either inserts the object into a database or finds an object with the
            relevant unique constraints if applicable.
        """
        for k in data.archived_urls:
            if data.archived_urls[k]:
                archived_url = data.archived_urls[k]
                filename = archived_url.split("/")[-1]
                ext = None if "." not in filename else filename.split(".")[-1].lower()

                media_kwargs = dict(
                    url=archived_url,
                    post=transformed.id,
                    raw_id=data.id,
                    original_url=k,
                    date=data.date,
                    date_archived=data.date_archived,
                    date_transformed=datetime.now(timezone.utc),
                    transformer=self.__version__,
                    scraper=data.scraper,
                    platform=data.platform,
                )

                if ext in ("mp4", "mov", "avi", "mkv"):
                    media_class = Video
                elif ext in ("oga", "mp3", "wav", "aif", "aiff", "aac"):
                    media_class = Audio
                elif ext in ("jpg", "jpeg", "png", "gif", "bmp", "heic", "tiff"):
                    media_class = Image
                else:
                    logger.warning(f"Unknown file extension {ext}")
                    media_class = Media
                insert(media_class(**media_kwargs))


class ETLController:
    """An ETLController will transform raw scraped data (ScrapedResult objects) into a more detailed format
    for analysis by using Transformer objects that have been registered with the controller.
    """

    posts_to_insert = []

    def __init__(self):
        self.transformers = []
        self.load_nlp()

    def register_transformer(self, transformer: Transformer):
        """Add a single Transformer instance to the list of available Transformers.

        Parameters
        ----------
        transformer : Transformer
            Instance of platform-specific Transformer to be controlled by the ETLController
        """

        self.transformers.append(transformer)

    def register_transformers(self, transformers):
        """Add a a list of Transformer instances to the list of available Transformers.

        Parameters
        ----------
        scrapers: <list>cisticola.scraper.Scraper
            List of instances of platform-specific Transformers to be controlled by the ETLController

        """
        for t in transformers:
            self.register_transformer(t)

    def connect_to_db(self, engine: Engine):
        """Connect the ETLController to a SQLAlchemy engine.

        Parameters
        ----------
        engine : sqlalchemy.engine.Engine
            Instance of SQLAlchemy Engine object to connect to
        """
        # create tables
        mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker(expire_on_commit=False)
        self.session.configure(bind=engine)

    # MAY4 can try adding some new functions for batching post inserts
    def flush_posts(self, session):
        """Save all outstanding posts to the database. For efficiency, instead of saving posts one at a time, the ETLController maintains a list of posts (``posts_to_insert``) and saves them in bulk.

        Parameters
        ----------
        session: sqlalchemy.orm.Session
            SQLAlchemy Session that interfaces with the database
        """
        session.bulk_save_objects(self.posts_to_insert)
        # logger.info(f"Bulk saved {len(self.posts_to_insert)} posts")
        self.posts_to_insert = []

    def load_nlp(self):
        """Load spaCy models into a dict."""
        kwargs = {"disable": ["parser", "tok2vec", "attribute_ruler"]}
        self.nlp_models = {
            "en": spacy.load("en_core_web_sm", **kwargs),
            "de": spacy.load("de_core_news_sm", **kwargs),
            "it": spacy.load("it_core_news_sm", **kwargs),
            "fr": spacy.load("fr_core_news_sm", **kwargs),
            "ru": spacy.load("ru_core_news_sm", **kwargs),
            "nl": spacy.load("nl_core_news_sm", **kwargs),
            "xx": spacy.load("xx_ent_wiki_sm", **kwargs),
        }

    def insert_post(self, obj, session, hydrate: bool = True, flush: bool = False):
        """Insert an object into the connected database.

        Parameters
        ----------
        obj:
            Instance of ORM-mapped class in the ``cisticola.base`` module to be inserted into the database
        session: sqlalchemy.orm.Session
            SQLAlchemy Session that interfaces with the database
        hydrate: bool
            If ``True``, additional data fields are extracted from the object and populated in the given database table
        flush: bool
            If ``True``, the object is returned with additional populated data fields (such as a primary key ID).
            If ``False``, the object is added to ``posts_to_insert`` and nothing is returned

        Returns
        -------
        None, or instance of ORM-mapped class from ``cisticola.base`` that has been inserted into the database, with additional data fields if ``flush`` argument is ``True``.
        """
        if hydrate and type(obj) == Post:
            obj.hydrate(nlp_models=self.nlp_models)
        elif hydrate and type(obj) != Video:
            obj.hydrate()

        if flush:
            self.flush_posts()

            session.add(obj)
            session.flush()

            logger.trace(f"Inserted new object {obj}")

            return obj
        else:
            self.posts_to_insert.append(obj)
            return None

    def insert_or_select(self, obj, session, hydrate: bool = True):
        """Insert an object into the database or return an existing object from the database.
        Regardless, the resulting object has an `id` attribute that can be referenced later.

        Parameters
        ----------
        obj:
            Instance of ORM-mapped class in the ``cisticola.base`` module to be inserted into the database
        session: sqlalchemy.orm.Session
            SQLAlchemy Session that interfaces with the database
        hydrate: bool
            If ``True``, additional data fields are extracted from the object and populated in the given database table

        Returns
        -------
        Object that has been inserted into the database, or existing object in the database, or None.

        """

        instance = None

        # This is using some adhoc unique constraints that might be worth formalizing at some point
        if type(obj) == Channel:
            instance = (
                session.query(Channel)
                .filter(
                    (
                        (
                            (Channel.url == obj.url)
                            & (Channel.url != "")
                            & (Channel.url is not None)
                            & (Channel.url != "https://t.me/s/")
                        )
                        | (
                            (Channel.platform_id == str(obj.platform_id))
                            & (Channel.platform_id != "")
                            & (Channel.platform_id is not None)
                        )
                        | (
                            (Channel.screenname == obj.screenname)
                            & (Channel.screenname != "")
                            & (Channel.screenname is not None)
                        )
                    )
                    & (Channel.platform == obj.platform)
                )
                .first()
            )

        elif type(obj) == Post:
            return self.insert_post(obj, session, hydrate)
            # instance = session.query(Post).filter_by(platform=obj.platform, platform_id=obj.platform_id).first()

        elif issubclass(type(obj), Media):
            instance = None
            # instance = session.query(type(obj)).filter_by(original_url=obj.original_url, post=obj.post).first()
            # if instance:
            #     logger.info(f"Found matching DB entry for {obj}: {instance}")
            #     return instance

            # instance = session.query(type(obj)).filter_by(original_url=obj.original_url).first()

            # # For Media objects we want to duplicate the entry to preserve the relationship with the post.
            # # However, we also want to avoid rehydration, hence the code below:
            # if instance:
            #     logger.info(f"Found matching media record, duplicating and inserting for new post")

            #     session.expunge(instance)
            #     make_transient(instance)
            #     instance.id = None
            #     instance.post = obj.post
            #     instance.raw_id = obj.raw_id

            #     session.add(instance)
            #     session.flush()
            #     return instance

        if instance:
            logger.info(f"Found matching DB entry for {obj}: {instance}")

            if type(obj) == Channel:
                if (
                    obj.source != instance.source
                    and obj.source == "linked_channel"
                    and instance.source != "researcher"
                    and (instance.source is None or instance.source[:4] != "snow")
                ):
                    logger.info(f"Updating source to linked channel")
                    instance.source = obj.source
                    instance.notes = obj.notes
                    instance.category = obj.category
                    instance.country = obj.country
                    instance.influencer = obj.influencer

                    session.flush()
                    session.commit()

                if instance.platform_id is None or instance.platform_id == "":
                    instance.platform_id = obj.platform_id
                    session.flush()
                    session.commit()

            return instance

        # Don't hydrate videos, because they can be quite large and this is time consuming, include spaCy models
        if hydrate and type(obj) == Post:
            obj.hydrate(nlp_models=self.nlp_models)
        elif hydrate and type(obj) != Video:
            obj.hydrate()

        session.add(obj)
        session.flush()

        logger.trace(f"Inserted new object {obj}")

        return obj

    @logger.catch(reraise=True)
    def transform_results(self, results: List[ScraperResult], hydrate: bool = True):
        """Transform raw ScraperResults objects into Post objects and
        Media objects. Then, adds them to the database.

        Parameters
        ----------
        results : List[ScraperResult]
            A list of ScraperResult objects to be transformed
        hydrate : bool
            Whether or not to fully hydrate transformed media. Default True.
        """
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        for result in results:
            if result.scraper is not None and result.platform is not None:
                for transformer in self.transformers:
                    handled = False

                    if transformer.can_handle(result):
                        logger.trace(
                            f"{transformer} is handling result {result.id} ({result.date})"
                        )
                        handled = True

                        transformer.transform(
                            result,
                            lambda obj: self.insert_or_select(obj, session, hydrate),
                            session,
                            lambda obj: self.insert_post(
                                obj, session, hydrate, flush=False
                            ),
                            lambda: self.flush_posts(session),
                        )

                        break

                if handled == False:
                    logger.warning(
                        f"No Transformer could handle ID {result.id} with platform {result.platform} ({result.date})"
                    )

        self.flush_posts(session)
        session.commit()

    @logger.catch(reraise=True)
    def transform_all_untransformed(
        self, hydrate: bool = True, min_date=datetime(2010, 1, 1)
    ):
        """Transform all ScraperResult objects in the database that do not have an
        equivalent Post object stored.

        Parameters
        ----------
        hydrate : bool
            Whether or not to fully hydrate transformed media. Default True.
        min_date: datetime.datetime
            Posts made before this date are not transformed.
        """

        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        BATCH_SIZE = 5000
        batch = []

        logger.info(f"Fetching first untransformed post batch of {BATCH_SIZE}")

        batch = (
            session.query(ScraperResult)
            .join(Post, isouter=True)
            .where(ScraperResult.date > min_date)
            .where(Post.raw_id == None)
            .order_by(ScraperResult.date.asc())
            .limit(BATCH_SIZE)
        ).all()

        while len(batch) > 0:
            logger.info(f"Found {len(batch)} items to ETL")

            self.transform_results(batch, hydrate=hydrate)

            logger.info(
                f"Fetching untransformed posts batch of {BATCH_SIZE}, offset {max(batch, key=lambda v: v.date).date}"
            )

            batch = (
                session.query(ScraperResult)
                .join(Post, isouter=True)
                .where(ScraperResult.date > min_date)
                .where(Post.raw_id == None)
                .where(ScraperResult.id != batch[-1].id)
                .where(ScraperResult.date >= batch[-1].date)
                .order_by(ScraperResult.date.asc())
                .limit(BATCH_SIZE)
            ).all()

    @logger.catch(reraise=True)
    def transform_info(self, results: List[ChannelInfo]):
        """Transform raw RawChannelInfo objects into ChannelInfo objects.

        Parameters
        ----------
        results : List[ChannelInfo]
            A list of ChannelInfo objects to be transformed
        """
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        for data in results:
            result = data.RawChannelInfo

            if result.scraper is not None and result.platform is not None:
                handled = False

                for transformer in self.transformers:
                    if transformer.can_handle(result):
                        logger.trace(
                            f"{transformer} is handling raw info result {result.id} ({result.date_archived})"
                        )
                        handled = True

                        transformer.transform_info(
                            result,
                            lambda obj: self.insert_or_select(obj, session, False),
                            session,
                            channel=data.Channel,
                        )

                        session.commit()
                        break

                    if handled == False:
                        logger.warning(
                            f"No Transformer could handle raw channel info ID {result.id} with platform {result.platform} ({result.date_archived})"
                        )

    @logger.catch(reraise=True)
    def transform_all_untransformed_info(self):
        """Transform all RawChannelInfo objects in the database that do not have an
        equivalent ChannelInfo object stored.
        """
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        BATCH_SIZE = 10000
        offset = 0
        batch = []

        query = (
            session.query(RawChannelInfo, Channel)
            .select_from(RawChannelInfo)
            .join(ChannelInfo, isouter=True)
            .join(Channel, RawChannelInfo.channel == Channel.id)
            .where(ChannelInfo.id == None)
            .order_by(RawChannelInfo.date_archived.asc())
        )

        while len(batch) > 0 or offset == 0:
            logger.info(
                f"Fetching untransformed info batch of {BATCH_SIZE}, offset {offset}"
            )

            batch = query.slice(offset, offset + BATCH_SIZE).all()
            offset += BATCH_SIZE

            logger.info(
                f"Found {len(batch)} info items to ETL ({offset} already processed)"
            )

            self.transform_info(batch)

    @logger.catch(reraise=True)
    def transform_media(self, results: List, hydrate: bool = True):
        """Transform raw ScraperResults objects into Post objects and
        Media objects, then add them to the database.

        Parameters
        ----------
        results : List[ScraperResult]
            A list of ScraperResult objects to be transformed
        hydrate : bool
            Whether or not to fully hydrate transformed media. Default ``True``.
        """
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        for total_result in results:
            result = total_result.ScraperResult
            if result.scraper is not None and result.platform is not None:
                for transformer in self.transformers:
                    handled = False

                    if transformer.can_handle(result):
                        logger.trace(
                            f"{transformer} is handling result {result.id} ({result.date})"
                        )
                        handled = True

                        transformer.transform_media(
                            result,
                            total_result.Post,
                            lambda obj: self.insert_or_select(obj, session, hydrate),
                        )

                        session.commit()
                        break

                if handled == False:
                    logger.warning(
                        f"No Transformer could handle ID {result.id} with platform {result.platform} ({result.date})"
                    )

    @logger.catch(reraise=True)
    def transform_all_untransformed_media(self, hydrate=True):
        """Transform all ScraperResult objects in the database that do not have an
        equivalent Post object stored.

        Parameters
        ----------
        hydrate : bool
            Whether or not to fully hydrate transformed media. Default True.
        """

        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        BATCH_SIZE = 50000

        logger.info(f"Fetching first untransformed post media batch of {BATCH_SIZE}")

        batch = (
            session.query(ScraperResult, Post)
            .join(Post)
            .join(Media, isouter=True)
            .filter(
                (ScraperResult.media_archived != None)
                & (cast(ScraperResult.archived_urls, String) != "{}")
                & (Media.id == None)
            )
            .order_by(ScraperResult.date.desc())
            .limit(BATCH_SIZE)
        ).all()

        while len(batch) > 0:
            logger.info(f"Found {len(batch)} items to ETL")

            self.transform_media(batch, hydrate=hydrate)

            logger.info(
                f"Fetching untransformed post media batch of {BATCH_SIZE}, offset {min(batch, key=lambda v: v.ScraperResult.date).ScraperResult.date}"
            )

            batch = (
                session.query(ScraperResult, Post)
                .join(Post)
                .join(Media, isouter=True)
                .where(
                    ScraperResult.date
                    <= min(batch, key=lambda v: v.ScraperResult.date).ScraperResult.date
                )
                .filter(
                    (ScraperResult.media_archived != None)
                    & (cast(ScraperResult.archived_urls, String) != "{}")
                    & (Media.id == None)
                )
                .order_by(ScraperResult.date.desc())
                .limit(BATCH_SIZE)
            ).all()
