from typing import List, Generator
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine

from cisticola.base import ScraperResult, TransformedResult, Media, mapper_registry

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

    def transform_media(self, data: ScraperResult, transformed: TransformedResult) -> Generator[Media, None, None]:
        """Yields Media objects from each piece of media present in a raw ScraperResult.
        
        Parameters
        ----------
        data : ScraperResult
            The ScraperResult object to process
        transformed : TransformedResult
            The TransformedResult version of `data`. (E.g. as generated by `Transformer.transform()`)

        Yields
        ------
        Media
            A media object generated from the ScraperResult. One ScraperResult can have multiple pieces
            of media contained within it, so this can generate an arbitrary number of Media objects
            (or their subclasses.) These Media objects are not fully hydrated.
        """
        
        pass

    def transform(data: ScraperResult) -> TransformedResult:
        """Transform a ScraperResult into a TransformedResult object. This extracts additional attributes
        that can be used directly for analysis.
        
        Parameters
        ----------
        data : ScraperResult
            The ScraperResult object to process.
        
        Returns
        -------
        TransformedResult
            A TransformedResult representation of the `data` object.
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

    @logger.catch
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

                    transformed = transformer.transform(result)

                    session.add(transformed)
                    session.flush()

                    media = transformer.transform_media(result, transformed)

                    count = 0
                    for obj in media:
                        if hydrate:
                            logger.info(f"Hydrating {obj}")
                            obj.hydrate()

                        session.add(obj)
                        count += 1

                    session.commit()
                    logger.info(f"{transformer} generated {count} media objects")
                    break

                if handled == False:
                    logger.warning(f"No Transformer could handle {result}")

    @logger.catch
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
        untransformed = session.query(ScraperResult).join(TransformedResult, isouter=True).where(TransformedResult.raw_id == None).all()
        logger.info(f"Found {len(untransformed)} items to ETL")

        self.transform_results(untransformed, hydrate=hydrate)