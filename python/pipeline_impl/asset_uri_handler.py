from pipeline.uri_handler import UriHandlerBase
from pipeline.uri import Uri
from pipeline.asset import Asset
from pipeline.director import Director

from typing import Union


class AssetUriHandler(UriHandlerBase):
    def __init__(self, director: Director):
        super(AssetUriHandler, self).__init__()
        self.__director = director

    def accepts(self, uri: Uri) -> bool:
        return uri.protocol == 'asset'

    def fetch(self, uri: Uri) -> Union[Asset, str]:
        ass = self.__director.get_asset(uri.path)
        if uri.query:
            if not hasattr(ass, uri.query):
                return ''
            return getattr(ass, uri.query)
        return ass

    def is_dynamic(self, uri: Uri) -> bool:
        return False
