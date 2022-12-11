from typing import Iterable, Tuple, List
from .asset_data import AssetData, AssetVersionData
from .future import FutureResult


class NotFoundError(RuntimeError):
    pass


class DataAccessInterface:
    def get_asset_data(self, asset_path_id: str) -> AssetData:
        asset_datas = self.get_asset_datas((asset_path_id,))
        if len(asset_datas) == 0:
            raise NotFoundError()
        return asset_datas[0]

    def get_asset_type_name(self, asset_path_id: str):
        raise NotImplementedError()

    def get_asset_version_data(self, asset_path_id: str, version_id: Tuple[int, int, int]) -> AssetVersionData:
        asset_ver_datas = self.get_asset_version_datas(((asset_path_id, version_id),))
        if len(asset_ver_datas) == 0:
            raise NotFoundError()
        return asset_ver_datas[0]

    def get_asset_version_data_from_path_id(self, asset_version_path_id: str) -> AssetVersionData:
        asset_version_datas = self.get_asset_version_datas_from_path_id((asset_version_path_id,))
        if len(asset_version_datas) == 0:
            raise NotFoundError()
        return asset_version_datas[0]

    def get_asset_datas(self, asset_path_ids: Iterable[str]) -> List[AssetData]:
        raise NotImplementedError()

    def get_asset_version_datas(self, asset_path_id_version_pairs: Iterable[Tuple[str, Tuple[int, int, int]]]) -> List[AssetVersionData]:
        raise NotImplementedError()

    def get_asset_version_datas_from_path_id(self, asset_version_path_id: Iterable[str]) -> List[AssetVersionData]:
        raise NotImplementedError()

    # setters
    def publish_new_asset_version(self, asset_path_id: str, version_data: AssetVersionData, dependencies: Iterable[str]) -> AssetVersionData:
        raise NotImplementedError()

    def create_new_asset(self, asset_type: str, asset_data: AssetData) -> AssetData:
        raise NotImplementedError()

    # scheduling execution
    def schedule_data_computation_for_asset_version(self, path_id: str) -> FutureResult:
        """
        if already scheduled - should return future to that process
        instead of scheduling multiple times
        but ultimately it's up to the implementation to decide
        """
        raise NotImplementedError()

    def _data_computation_completed_callback(self, path_id: str, data):
        """
        whatever was scheduled for data computation should call this
        """
        raise NotImplementedError()

    # dependencies
    def get_version_dependencies(self, version_path_id: str) -> Iterable[str]:
        """
        get path_ids for versions dependent on given
        """
        raise NotImplementedError()

    def get_dependent_versions(self, version_path_id: str) -> Iterable[str]:
        """
        get path_ids for versions that depend on given
        """
        raise NotImplementedError()

    def add_dependencies(self, version_path_id: str, dependency_path_ids: Iterable[str]):
        """
        add dependencies to given version_path_id
        """
        raise NotImplementedError()

    def remove_dependencies(self, version_path_id: str, dependency_path_ids: Iterable[str]):
        """
        remove dependencies from a given version_path_id
        if given dependency does not exist - this function should ignore it
        """
        raise NotImplementedError()


