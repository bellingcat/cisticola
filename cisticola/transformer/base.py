from typing import List, Generator, Union, Callable
from loguru import logger
from sqlalchemy import cast, String
from sqlalchemy.orm import sessionmaker, make_transient
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.expression import func
from collections import defaultdict
from datetime import datetime, timezone

from cisticola.base import RawChannelInfo, ChannelInfo, ScraperResult, Post, Media, Channel, mapper_registry, Image, Video, Audio


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
            True if it can be handled by this Transformer, false otherwise.
        """

        pass

    def transform(data: ScraperResult, insert: Callable) -> Generator[Union[Post, Channel, Media], None, None]:
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
        '''Transform media'''
        for k in data.archived_urls:
            if data.archived_urls[k]:
                archived_url = data.archived_urls[k]
                filename = archived_url.split('/')[-1]
                ext = None if '.' not in filename else filename.split('.')[-1].lower()

                if ext == 'mp4' or ext == 'mov' or ext == 'avi' or ext =='mkv':
                    insert(Video(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k, date=data.date, date_archived=data.date_archived, date_transformed=datetime.now(timezone.utc), transformer=self.__version__, scraper=data.scraper, platform=data.platform))
                elif ext == 'oga' or ext == 'mp3' or ext == "wav" or ext == 'aif' or ext == 'aiff' or ext == 'aac':
                    insert(Audio(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k, date=data.date, date_archived=data.date_archived, date_transformed=datetime.now(timezone.utc), transformer=self.__version__, scraper=data.scraper, platform=data.platform))
                elif ext == 'jpg' or ext == 'jpeg' or ext == 'png' or ext == 'gif' or ext == 'bmp' or ext == 'heic' or ext == 'tiff':
                    insert(Image(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k, date=data.date, date_archived=data.date_archived, date_transformed=datetime.now(timezone.utc), transformer=self.__version__, scraper=data.scraper, platform=data.platform))
                else:
                    logger.warning(f"Unknown file extension {ext}")
                    insert(Media(url=archived_url, post=transformed.id, raw_id=data.id, original_url=k, date=data.date, date_archived=data.date_archived, date_transformed=datetime.now(timezone.utc), transformer=self.__version__, scraper=data.scraper, platform=data.platform))


class ETLController:
    """An ETLController will transform raw scraped data (ScrapedResult objects) into a more detailed format
    for analysis by using Transformer objects that have been registered with the controller.
    """

    def __init__(self):
        self.transformers = []

    def register_transformer(self, transformer: Transformer):
        """Adds a Transformer to the list of available Transformers.

        Parameters
        ----------
        transformer : Transformer
            The Transformer to register
        """

        self.transformers.append(transformer)

    def register_transformers(self, transformers):
        for t in transformers:
            self.register_transformer(t)

    def connect_to_db(self, engine: Engine):
        """Connects the ETLController to a SQLAlchemy engine.

        Parameters
        ----------
        engine : Engine
            SQLAlchemy Engine object
        """
        # create tables
        mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.session.configure(bind=engine)

    def insert_or_select(self, obj, session, hydrate: bool = True):
        """Inserts an object into the database or returns an existing object from the database.
        Regardless, the resulting object has an `id` attribute that can be referenced later."""

        instance = None

        # This is using some adhoc unique constraints that might be worth formalizing at some point
        if type(obj) == Channel:
            instance = session.query(Channel).filter(
                (((Channel.url==obj.url)&(Channel.url!='')&(Channel.url is not None)&(Channel.url!='https://t.me/s/'))|
                ((Channel.platform_id==str(obj.platform_id))&(Channel.platform_id!='')&(Channel.platform_id is not None))|
                ((Channel.screenname==obj.screenname)&(Channel.screenname!='')&(Channel.screenname is not None)))&
                (Channel.platform==obj.platform)).first()
            
        elif type(obj) == Post:
            instance = None
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
                if obj.source != instance.source and obj.source == 'linked_channel' and instance.source != 'researcher' and (instance.source is None or instance.source[:4] != 'snow'):
                    logger.info(f"Updating source to linked channel")
                    instance.source = obj.source
                    instance.notes = obj.notes
                    instance.category = obj.category
                    instance.country = obj.country
                    instance.influencer = obj.influencer
                    session.flush()
                    session.commit()

            return instance

        # Don't hydrate videos, because they can be quite large and this is time consuming
        if hydrate and type(obj) != Video:
            obj.hydrate()

        session.add(obj)
        session.flush()

        logger.trace(f"Inserted new object {obj}")

        return obj

    @logger.catch(reraise=True)
    def transform_results(self, results: List[ScraperResult], hydrate: bool = True):
        """Transforms raw ScraperResults objects into Post objects and
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
                        logger.trace(f"{transformer} is handling result {result.id} ({result.date})")
                        handled = True

                        transformer.transform(result, lambda obj: self.insert_or_select(obj, session, hydrate), session)

                        session.commit()
                        break

                if handled == False:
                    logger.warning(f"No Transformer could handle ID {result.id} with platform {result.platform} ({result.date})")

    @logger.catch(reraise=True)
    def transform_all_untransformed(self, hydrate: bool = True):
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

        BATCH_SIZE = 5000
        offset = 0
        batch = []

        logger.info(f"Fetching first untransformed post batch of {BATCH_SIZE}")

        batch = (session.query(ScraperResult)
            .join(Post, isouter=True)
            .where(ScraperResult.id > 35000000) # TODO this can be a CLI argument or something
            .where(Post.raw_id == None)
            .order_by(ScraperResult.date.asc())
            .limit(BATCH_SIZE)
        ).all()

        while len(batch) > 0:
            logger.info(f"Found {len(batch)} items to ETL")

            self.transform_results(batch, hydrate=hydrate)

            logger.info(f"Fetching untransformed posts batch of {BATCH_SIZE}, offset {max(batch, key=lambda v: v.date).date}")

            batch = (query(ScraperResult)
                    .join(Post, isouter=True)
                    .where(ScraperResult.id > 35000000) # TODO this can be a CLI argument or something
                    .where(Post.raw_id == None)
                    .where(ScraperResult.date >= max(batch, key=lambda v: v.date).date)
                    .order_by(ScraperResult.date.asc())
                    .limit(BATCH_SIZE)
                ).all()



    @logger.catch(reraise=True)
    def transform_info(self, results: List[ChannelInfo]):
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
                        logger.trace(f"{transformer} is handling raw info result {result.id} ({result.date_archived})")
                        handled = True

                        transformer.transform_info(result, lambda obj: self.insert_or_select(obj, session, False), session, channel=data.Channel)

                        session.commit()
                        break

                    if handled == False:
                        logger.warning(f"No Transformer could handle raw channel info ID {result.id} with platform {result.platform} ({result.date_archived})")


    @logger.catch(reraise=True)
    def transform_all_untransformed_info(self):
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()

        BATCH_SIZE = 10000
        offset = 0
        batch = []

        query = (session.query(RawChannelInfo, Channel)
            .select_from(RawChannelInfo)
            .join(ChannelInfo, isouter=True)
            .join(Channel, RawChannelInfo.channel==Channel.id)
            .where(ChannelInfo.id == None)
            .order_by(RawChannelInfo.date_archived.asc())
        )

        while len(batch) > 0 or offset == 0:
            logger.info(f"Fetching untransformed info batch of {BATCH_SIZE}, offset {offset}")

            batch = query.slice(offset, offset + BATCH_SIZE).all()
            offset += BATCH_SIZE

            logger.info(f"Found {len(batch)} info items to ETL ({offset} already processed)")

            self.transform_info(batch)

    @logger.catch(reraise=True)
    def transform_media(self, results: List, hydrate: bool = True):
        """Transforms raw ScraperResults objects into Post objects and
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

        for total_result in results:
            result = total_result.ScraperResult
            if result.scraper is not None and result.platform is not None:
                for transformer in self.transformers:
                    handled = False

                    if transformer.can_handle(result):
                        logger.trace(f"{transformer} is handling result {result.id} ({result.date})")
                        handled = True

                        transformer.transform_media(result, total_result.Post, lambda obj: self.insert_or_select(obj, session, hydrate))

                        session.commit()
                        break

                if handled == False:
                    logger.warning(f"No Transformer could handle ID {result.id} with platform {result.platform} ({result.date})")

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

        batch = (session.query(ScraperResult, Post)
            .join(Post)
            .join(Media, isouter=True)
            .filter((ScraperResult.media_archived != None) & (cast(ScraperResult.archived_urls, String) != '{}') & (Media.id == None))
            .order_by(ScraperResult.date.desc())
            .limit(BATCH_SIZE)
        ).all()

        while len(batch) > 0:
            logger.info(f"Found {len(batch)} items to ETL")

            self.transform_media(batch, hydrate=hydrate)

            logger.info(f"Fetching untransformed post media batch of {BATCH_SIZE}, offset {min(batch, key=lambda v: v.ScraperResult.date).ScraperResult.date}")

            batch = (session.query(ScraperResult, Post)
                .join(Post)
                .join(Media, isouter=True)
                .where(ScraperResult.date <= min(batch, key=lambda v: v.ScraperResult.date).ScraperResult.date)
                .filter((ScraperResult.media_archived != None) & (cast(ScraperResult.archived_urls, String) != '{}') & (Media.id == None))
                .order_by(ScraperResult.date.desc())
                .limit(BATCH_SIZE)
            ).all()
