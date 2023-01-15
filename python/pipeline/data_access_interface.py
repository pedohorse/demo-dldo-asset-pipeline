from pathlib import Path
from typing import Iterable, Tuple, List, Optional
from .asset_data import AssetData, AssetVersionData, AssetTemplateData
from .task_scheduling_interface import TaskSchedulingInterface
from .future import FutureResult


class NotFoundError(RuntimeError):
    pass


class TaskSchedulerNotAvailable(RuntimeError):
    """
    base exception for everything related to not being able to connect to data computation mechanism
    """
    pass


class DataAccessInterface:
    def __init__(self, task_scheduler: TaskSchedulingInterface):
        self.__task_scheduler = task_scheduler

    def get_asset_data(self, asset_path_id: str) -> AssetData:
        asset_datas = self.get_asset_datas((asset_path_id,))
        if len(asset_datas) == 0:
            raise NotFoundError()
        return asset_datas[0]

    def get_asset_type_name(self, asset_path_id: str):
        raise NotImplementedError()

    def get_asset_version_data(self, asset_path_id: str, version_id: Optional[Tuple[int, int, int]]) -> AssetVersionData:
        """
        if version_id is None - fetch the latest

        :param asset_path_id:
        :param version_id:
        :return:
        """
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

    def get_asset_version_datas(self, asset_path_id_version_pairs: Iterable[Tuple[str, Optional[Tuple[int, int, int]]]]) -> List[AssetVersionData]:
        raise NotImplementedError()

    def get_asset_version_datas_from_path_id(self, asset_version_path_id: Iterable[str]) -> List[AssetVersionData]:
        raise NotImplementedError()

    def get_leaf_asset_version_pathids(self) -> List[str]:
        """
        get ALL asset versions that NOTHING DEPENDS ON

        :return:
        """
        raise NotImplementedError()

    # setters
    def publish_new_asset_version(self, asset_path_id: str, version_data: AssetVersionData, dependencies: Iterable[str]) -> AssetVersionData:
        raise NotImplementedError()

    def create_new_asset(self, asset_type: str, asset_data: AssetData) -> AssetData:
        raise NotImplementedError()

    # templates
    def get_asset_template_data_for_asset_path_id(self, asset_path_id: str) -> AssetTemplateData:
        datas = self.get_asset_template_datas_for_asset_path_id([asset_path_id])
        if len(datas) == 0:
            raise NotFoundError()
        return datas[0]

    def get_asset_template_datas_for_asset_path_id(self, asset_path_ids: Iterable[str]) -> List[AssetTemplateData]:
        """
        get templates for generating versions of assets defined by asset_path_ids
        """
        raise NotImplementedError()

    def create_asset_template(self, asset_template_data: AssetTemplateData,
                                    trigger_asset_path_ids: Iterable[str],
                                    asset_version_dependencies: Iterable[str]) -> AssetTemplateData:
        """
        create new asset template, that will be triggered by any asset from input_asset_path_ids
        """
        raise NotImplementedError()

    def update_asset_template_data(self, asset_template_data: AssetTemplateData):
        raise NotImplementedError()

    def get_asset_templates_triggered_by(self, asset_path_id: str) -> List[AssetTemplateData]:
        """
        get all asset templates that will be triggered by a change in "asset_path_id" asset
        """
        raise NotImplementedError()

    # scheduling execution
    def get_task_scheduler(self):
        return self.__task_scheduler

    def schedule_data_computation_for_asset_version(self, path_id: str) -> FutureResult:
        """
        if already scheduled - should return future to that process
        instead of scheduling multiple times
        but ultimately it's up to the implementation to decide
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

    def remove_dependencies(self, version_path_id: str, dependency_path_ids: Iterable[str]):  # TODO: remove?
        """
        remove dependencies from a given version_path_id
        if given dependency does not exist - this function should ignore it
        """
        raise NotImplementedError()

    def get_template_fixed_dependencies(self, asset_path_id: str) -> Iterable[str]:
        """
        a template has triggers (assets), and fixed deps (asset versions)
        """
        raise NotImplementedError()

    # files location
    def get_pipeline_render_root(self) -> Path:
        raise NotImplementedError()

    def get_pipeline_cache_root(self) -> Path:
        raise NotImplementedError()

    def get_pipeline_source_root(self) -> Path:
        raise NotImplementedError()
