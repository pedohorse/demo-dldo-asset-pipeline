from pipeline.uri_handler import UriHandlerBase
from pipeline.uri import Uri
from pipeline.asset import Asset, AssetVersion
from pipeline.director import Director

from typing import Union


class AssetVersionUriHandler(UriHandlerBase):
    def __init__(self, director: Director):
        super(AssetVersionUriHandler, self).__init__()
        self.__director = director

    def accepts(self, uri: Uri) -> bool:
        return uri.protocol == 'assetver'

    def fetch(self, uri: Uri) -> Union[AssetVersion, str]:
        assver = self.__director.get_asset_version(uri.path)
        if uri.query:
            if not hasattr(assver, uri.query):
                return ''
            return getattr(assver, uri.query)
        return assver
