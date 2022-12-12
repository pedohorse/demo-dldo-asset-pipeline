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

    def is_dynamic(self, uri: Uri) -> bool:
        """
        Some URI may resolve to different things depending on something.
        It's important to distinguish such URIs in case you would want to lock them for reproducibility

        :param uri:
        :return:
        """
        raise NotImplementedError()
