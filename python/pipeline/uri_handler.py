from .uri import Uri

from typing import Any


class UriNotSupportedError(RuntimeError):
    def __init__(self, uri: Uri):
        self.uri = uri


class UriHandlerBase:

    def accepts(self, uri: Uri) -> bool:
        raise NotImplementedError()

    def fetch(self, uri: Uri) -> Any:
        """
        Fetches whatever is represented by the given URI

        :param uri:
        :return:
        """
        raise NotImplementedError()
