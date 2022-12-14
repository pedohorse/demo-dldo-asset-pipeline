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

    def fetch(self, uri: Uri) -> Union[AssetVersion, str]:
        try:
            assver = self.__director.get_asset_version(uri.path)
        except NotFoundError:
            # maybe uri path is an asset path, then we bring the default version (may be latest, may be locked)
            ass = self.__director.get_asset(uri.path)
            assver = ass.get_default_version()
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
