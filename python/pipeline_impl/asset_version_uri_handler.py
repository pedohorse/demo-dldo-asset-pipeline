from pipeline.uri_handler import UriHandlerBase
from pipeline.uri import Uri
from pipeline.asset import Asset, AssetVersion
from pipeline.director import Director, NotFoundError

from typing import Union


class AssetVersionUriHandler(UriHandlerBase):
    def __init__(self, director: Director):
        super(AssetVersionUriHandler, self).__init__()
        self.__director = director

    def accepts(self, uri: Uri) -> bool:
        return uri.protocol == 'assetver'

    def fetch(self, uri: Uri) -> Union[AssetVersion, str]:
        try:
            assver = self.__director.get_asset_version(uri.path)
        except NotFoundError:
            # maybe uri path is an asset path, then we bring the latest version
            ass = self.__director.get_asset(uri.path)
            assver = ass.get_latest_version()
        if uri.query:
            if not hasattr(assver, uri.query):
                return ''
            return getattr(assver, uri.query)
        return assver
