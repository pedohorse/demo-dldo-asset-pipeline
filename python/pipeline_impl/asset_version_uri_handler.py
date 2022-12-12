import os
import json
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

    def _get_lock_mapped_pathid(self, uri: Uri) -> Union[str, None]:
        """
        check environment if there is a lock for a given dynamic uri
        None if there's no lock
        """
        if not self.is_dynamic(uri):
            return None
        mapping = json.loads(os.environ.get('LBATTR_locked_asset_versions', '{}'))
        return mapping.get(str(uri))

    def fetch(self, uri: Uri) -> Union[AssetVersion, str]:
        locked_pathid = self._get_lock_mapped_pathid(uri)
        try:
            assver = self.__director.get_asset_version(locked_pathid or uri.path)
        except NotFoundError:
            if locked_pathid:  # if locked - we do NOT try to resolve dynamically
                raise
            # maybe uri path is an asset path, then we bring the latest version
            ass = self.__director.get_asset(uri.path)
            assver = ass.get_latest_version()
        if uri.query:
            if not hasattr(assver, uri.query):
                return ''
            return getattr(assver, uri.query)
        return assver

    def is_dynamic(self, uri: Uri) -> bool:
        """
        dynamic asset version is one without
        here we do NOT want to consider uri not dynamic if there is a lock for it
            cuz that can cause next publisher to not lock such uri

        :param uri:
        :return:
        """
        try:
            assver = self.__director.get_asset_version(uri.path)
            return False
        except NotFoundError:  # cuz it's not a version - it must be an asset
            ass = self.__director.get_asset(uri.path)
            return True
