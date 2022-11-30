from pipeline.uri_handler import UriHandlerBase, UriNotSupportedError
from pipeline.uri import Uri

from typing import List, Iterable, Optional, Any


class UriMultiHandler(UriHandlerBase):
    def __init__(self, handlers: Optional[Iterable[UriHandlerBase]]):
        super(UriMultiHandler, self).__init__()
        self.__handlers: List[UriHandlerBase] = list(handlers) or []

    def accepts(self, uri: Uri) -> bool:
        return any(h.accepts(uri) for h in self.__handlers)

    def fetch(self, uri: Uri) -> Any:
        for handler in self.__handlers:
            if handler.accepts(uri):
                return handler.fetch(uri)
        raise UriNotSupportedError(uri)
