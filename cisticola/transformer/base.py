from typing import List, Generator
from loguru import logger
from sqlalchemy.orm import sessionmaker

from cisticola.base import ScraperResult, TransformedResult, Media, mapper_registry

class Transformer:
    """Interface class for transformers"""

    __version__ = "Transformer 0.0.0"

    def __init__(self):
        pass

    def can_handle(data: ScraperResult) -> bool:
        pass

    def transform_media(self, data: ScraperResult, transformed: TransformedResult) -> Generator[Media, None, None]:
        pass

    def transform(data: ScraperResult) -> TransformedResult:
        pass


class ETLController:
    """This class will transform the raw_data tables into a format more conducive to analysis."""

    def __init__(self):
        self.transformers = []

    def register_transformer(self, transformer: Transformer):
        self.transformers.append(transformer)

    def connect_to_db(self, engine):
        # create tables
        mapper_registry.metadata.create_all(bind=engine)

        self.session = sessionmaker()
        self.session.configure(bind=engine)

    @logger.catch
    def transform_results(self, results: List[ScraperResult], hydrate: bool = True):
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
        if self.session is None:
            logger.error("No DB session")
            return

        session = self.session()
        untransformed = session.query(ScraperResult).join(TransformedResult, isouter=True).where(TransformedResult.raw_id == None).all()
        logger.info(f"Found {len(untransformed)} items to ETL")

        self.transform_results(untransformed, hydrate=hydrate)