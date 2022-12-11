from .asset_data import AssetData, AssetVersionData
from .data_access_interface import DataAccessInterface, NotFoundError
from .asset import Asset, AssetVersion
from .uri_handler import UriHandlerBase, UriNotSupportedError
from .uri import Uri

from typing import Iterable, Optional, Type, Union, List, Tuple, Dict


class Director:
    """
    main configurator class
    """
    def __init__(self, data_accessor: DataAccessInterface, uri_handler: Iterable[UriHandlerBase] = ()):
        self.__data_accessor: DataAccessInterface = data_accessor
        self.__uri_handler: List[UriHandlerBase] = list(uri_handler) if uri_handler else []
        self.__asset_types: Dict[str, Type[Asset]] = {}

    def register_asset_type(self, asset_type: Type[Asset], asset_type_name: Optional[str] = None):
        self.__asset_types[asset_type_name or asset_type.type_name()] = asset_type

    def register_uri_handler(self, uri_handler):
        if uri_handler in self.__uri_handler:
            return
        self.__uri_handler.append(uri_handler)

    def get_asset_version(self, path_id: str) -> AssetVersion:
        asset_ver_data = self.__data_accessor.get_asset_version_data_from_path_id(path_id)
        type_name = self.__data_accessor.get_asset_type_name(asset_ver_data.asset_path_id)
        version_class = self.__asset_types[type_name]._get_version_class()
        return version_class.from_path_id(self.__data_accessor, path_id)

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
        return self.__asset_types[type_name](self.__data_accessor.create_new_asset(type_name, asset_data).path_id, self.__data_accessor)

    def get_data_accessor(self) -> DataAccessInterface:
        return self.__data_accessor

    def get_uri_handlers(self) -> Tuple[UriHandlerBase]:
        return tuple(self.__uri_handler)

    def fetch_uri(self, uri: Union[Uri, str]):
        if isinstance(uri, str):
            uri = Uri(uri)
        for handler in self.__uri_handler:
            if handler.accepts(uri):
                return handler.fetch(uri)
        raise UriNotSupportedError(uri)
