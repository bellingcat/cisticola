from typing import List, Generator, Union, Callable
from loguru import logger
from sqlalchemy.orm import sessionmaker, make_transient
from sqlalchemy.engine.base import Engine
from collections import defaultdict

from cisticola.base import ScraperResult, TransformedResult, Media, Channel, mapper_registry


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

    def transform(data: ScraperResult, insert: Callable) -> Generator[Union[TransformedResult, Channel, Media], None, None]:
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
            instance = session.query(Channel).filter_by(url=obj.url, platform_id=obj.platform_id, platform=obj.platform).first()
            
        elif type(obj) == TransformedResult:
            instance = session.query(TransformedResult).filter_by(platform=obj.platform, platform_id=obj.platform_id).first()

        elif issubclass(type(obj), Media):
            instance = session.query(type(obj)).filter_by(original_url=obj.original_url, post=obj.post).first()
            if instance:
                logger.info(f"Found matching DB entry for {obj}: {instance}")
                return instance

            instance = session.query(type(obj)).filter_by(original_url=obj.original_url).first()
            
            # For Media objects we want to duplicate the entry to preserve the relationship with the post.
            # However, we also want to avoid rehydration, hence the code below:
            if instance:
                logger.info(f"Found matching media record, duplicating and inserting for new post")

                session.expunge(instance) 
                make_transient(instance) 
                instance.id = None 
                instance.post = obj.post
                instance.raw_id = obj.raw_id

                session.add(instance)
                session.flush()
                return instance

        if instance:
            logger.info(f"Found matching DB entry for {obj}: {instance}")
            return instance

        if hydrate:
            obj.hydrate()

        logger.info(f"Inserting new object {obj}")
        session.add(obj)
        session.flush()
        return obj

    @logger.catch(reraise=True)
    def transform_results(self, results: List[ScraperResult], hydrate: bool = True):
        """Transforms raw ScraperResults objects into TransformedResult objects and
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

        for result in results:
            for transformer in self.transformers:
                handled = False

                if transformer.can_handle(result):
                    logger.info(f"{transformer} is handling result {result}")
                    handled = True
                    session = self.session()

                    transformer.transform(result, lambda obj: self.insert_or_select(obj, session, hydrate))
                    session.commit()
                    break

                if handled == False:
                    logger.warning(f"No Transformer could handle {result}")

    @logger.catch(reraise=True)
    def transform_all_untransformed(self, hydrate: bool = True):
        """Transform all ScraperResult objects in the database that do not have an
        equivalent TransformedResult object stored.

        Parameters
        ----------
        hydrate : bool
            Whether or not to fully hydrate transformed media. Default True.
        """

        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()
        untransformed = (
            session.query(ScraperResult)
            .join(TransformedResult, isouter=True)
            .where(TransformedResult.raw_id == None)
            .all()
        )
        logger.info(f"Found {len(untransformed)} items to ETL")

        self.transform_results(untransformed, hydrate=hydrate)
