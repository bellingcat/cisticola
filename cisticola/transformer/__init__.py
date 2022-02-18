import cisticola.base

class Transformer:
    """Interface class for transformers"""

    __version__ = "Transformer 0.0.0"

    def __init__(self):
        pass

    def can_handle(data: cisticola.base.ScraperResult) -> bool:
        pass

    def transform(data: cisticola.base.ScraperResult) -> cisticola.base.TransformedResult:
        pass

