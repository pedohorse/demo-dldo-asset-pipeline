from .asset_data import AssetData, AssetVersionData
from .data_access_interface import DataAccessInterface
from .asset import Asset, AssetVersion
from .uri_handler import UriHandlerBase
from .uri import Uri

from typing import Iterable, Optional


class Director:
    """
    main configurator class
    """
    def __init__(self, data_accessor: DataAccessInterface, uri_handler: UriHandlerBase):
        self.__data_accessor: DataAccessInterface = data_accessor
        self.__uri_handler: UriHandlerBase = uri_handler

    def get_asset_version(self, path_id: str):
        return AssetVersion.from_path_id(self.__data_accessor, path_id)

    def get_asset(self, path_id: str):
        return Asset(path_id, self.__data_accessor)

    def new_asset(self, name: str, description: str):
        asset_data = AssetData(None,
                               name,
                               description)
        return Asset(self.__data_accessor.create_new_asset(asset_data).path_id, self.__data_accessor)

    def get_data_accessor(self) -> DataAccessInterface:
        return self.__data_accessor

    def get_uri_handler(self) -> UriHandlerBase:
        return self.__uri_handler

    def fetch_uri(self, uri: Uri):
        return self.__uri_handler.fetch(uri)

