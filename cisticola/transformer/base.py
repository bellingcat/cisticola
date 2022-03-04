from cisticola.base import ScraperResult, TransformedResult

class Transformer:
    """Interface class for transformers"""

    __version__ = "Transformer 0.0.0"

    def __init__(self):
        pass

    def can_handle(data: ScraperResult) -> bool:
        pass

    def transform(data: ScraperResult) -> TransformedResult:
        pass

