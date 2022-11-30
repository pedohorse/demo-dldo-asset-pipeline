from pipeline.uri_handler import UriHandlerBase
from pipeline.uri import Uri
from pipeline.asset import Asset
from pipeline.data_access_interface import DataAccessInterface


class AssetUriHandler(UriHandlerBase):
    def __init__(self, data_accessor: DataAccessInterface):
        super(AssetUriHandler, self).__init__()
        self.__data_access_interface = data_accessor

    def accepts(self, uri: Uri) -> bool:
        return uri.protocol == 'asset'

    def fetch(self, uri: Uri) -> Asset:
        return Asset(uri.path, self.__data_access_interface)
