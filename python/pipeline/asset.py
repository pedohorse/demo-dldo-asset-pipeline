from .asset_data import AssetData, AssetVersionData, DataState
from .data_access_interface import DataAccessInterface
from .future import FutureResult, CompletedFuture

from typing import Union, Tuple, List, Optional

VersionType = Union[int, Tuple[int], Tuple[int, int], Tuple[int, int, int]]


class DataNotYetAvailable(Exception):
    pass


def _normalize_version(version_id: VersionType) -> Tuple[int, int, int]:
    if isinstance(version_id, int):
        return version_id, -1, -1
    if len(version_id) >= 3:
        return version_id[:3]
    return *version_id, *((-1,)*(3-len(version_id)))

def _denormalize_version(version_id: Tuple[int, int, int]) -> VersionType
    if version_id[1] == -1:
        return version_id[0]
    if version_id[2] == -1:
        return version_id[:2]
    return version_id


class Asset:
    def __init__(self, asset_path_id: str, data_provider: DataAccessInterface):
        self.__asset_data: AssetData = data_provider.get_asset_data(asset_path_id)
        self.__data_provider = data_provider

    @property
    def path_id(self):
        return self.__asset_data.path_id

    @property
    def name(self):
        return self.__asset_data.name

    @property
    def description(self):
        return self.__asset_data.description

    def get_version(self, version_id: VersionType) -> "AssetVersion":
        version_id = _normalize_version(version_id)

        return AssetVersion(self, version_id)

    def create_new_version(self, version_id: Optional[VersionType] = None):
        version_id = _normalize_version(version_id)
        version_data = AssetVersionData(None,
                                        self.path_id,
                                        version_id,
                                        {},
                                        DataState.NOT_COMPUTED,
                                        None,
                                        None)
        version_data = self._get_data_provider().publish_new_asset_version(self.path_id, version_data)
        return AssetVersion(self, version_data.version_id)

    def _get_data_provider(self) -> DataAccessInterface:
        return self.__data_provider


class AssetVersion:
    def __init__(self, asset: Asset, version_id: VersionType):
        self.__asset = asset
        self.__version_id = _normalize_version(version_id)
        self._fresh_asset_version_data()  # this will raise if asset+version_id are invalid

    @classmethod
    def from_path_id(cls, data_provider: DataAccessInterface, version_path_id: str) -> "AssetVersion":
        data = data_provider.get_asset_version_data_from_path_id(version_path_id)
        return AssetVersion(Asset(data.asset_path_id, data_provider), data.version_id)

    @property
    def version_id(self) -> VersionType:
        return _denormalize_version(self.__version_id)

    @property
    def asset(self) -> Asset:
        return self.__asset

    @property
    def data_provider(self) -> DataAccessInterface:
        return self.__asset._get_data_provider()

    def _fresh_asset_version_data(self) -> AssetVersionData:
        return self.data_provider.get_asset_version_data(self.__asset.path_id, self.__version_id)

    def schedule_data_calculation_if_needed(self) -> FutureResult:
        data = self._fresh_asset_version_data()
        if data.data_availability != DataState.AVAILABLE:
            return CompletedFuture(True)
        return self.data_provider.schedule_data_computation_for_asset_version(data.path_id)

    # AssetVersionData access
    @property
    def path_id(self):
        data = self._fresh_asset_version_data()
        return data.path_id

    def is_data_available(self,):
        return self._fresh_asset_version_data().data_availability == DataState.AVAILABLE

    def get_data(self):
        data = self._fresh_asset_version_data()
        return data.data

    def has_field(self, key: str):
        if not self.is_data_available():
            raise DataNotYetAvailable()
        data = self._fresh_asset_version_data()
        return key in data.data

    def get_field(self, key: str):
        if not self.is_data_available():
            raise DataNotYetAvailable()
        data = self._fresh_asset_version_data()
        return data.data[key]

    def get_dependencies(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        return [self.from_path_id(self.data_provider, x) for x in self.data_provider.get_version_dependencies(data.path_id)]

    def get_dependants(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        return [self.from_path_id(self.data_provider, x) for x in self.data_provider.get_dependent_versions(data.path_id)]