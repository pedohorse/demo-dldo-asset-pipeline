from .asset_data import AssetData, AssetVersionData
from .data_access_interface import DataAccessInterface, NotFoundError
from .asset import Asset, AssetVersion
from .uri_handler import UriHandlerBase
from .uri import Uri

from typing import Iterable, Optional, Type


class Director:
    """
    main configurator class
    """
    def __init__(self, data_accessor: DataAccessInterface, uri_handler: UriHandlerBase):
        self.__data_accessor: DataAccessInterface = data_accessor
        self.__uri_handler: UriHandlerBase = uri_handler
        self.__asset_types = {}

    def register_asset_type(self, asset_type: Type[Asset], asset_type_name: Optional[str] = None):
        self.__asset_types[asset_type_name or asset_type.type_name()] = asset_type

    def get_asset_version(self, path_id: str) -> AssetVersion:
        return AssetVersion.from_path_id(self.__data_accessor, path_id)

    def get_asset(self, path_id: str) -> Asset:
        type_name = self.__data_accessor.get_asset_type_name(path_id)
        if type_name not in self.__asset_types:
            raise NotFoundError(type_name)  # should probably change this exception type
        return self.__asset_types[type_name](path_id, self.__data_accessor)

    def new_asset(self, name: str, description: str, type_name: str, path_id: str) -> Asset:
        asset_data = AssetData(path_id,
                               name,
                               description)
        if type_name not in self.__asset_types:
            raise NotFoundError(type_name)  # should probably change this exception type
        return self.__asset_types[type_name](self.__data_accessor.create_new_asset(asset_data).path_id, self.__data_accessor)

    def get_data_accessor(self) -> DataAccessInterface:
        return self.__data_accessor

    def get_uri_handler(self) -> UriHandlerBase:
        return self.__uri_handler

    def fetch_uri(self, uri: Uri):
        return self.__uri_handler.fetch(uri)
