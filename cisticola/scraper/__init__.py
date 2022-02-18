from typing import List
import cisticola.base


class Scraper:
    __version__ = "Scraper 0.0.0"

    def __init__(self):
        pass

    def __str__(self):
        return self.__version__

    def can_handle(self, channel: cisticola.base.Channel) -> bool:
        pass

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> List[cisticola.base.ScraperResult]:
        pass
