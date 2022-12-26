import json
import os
from .asset_data import AssetData, AssetVersionData, DataState, AssetTemplateData
from .data_access_interface import DataAccessInterface
from .future import FutureResult, CompletedFuture
from .utils import normalize_version, denormalize_version, VersionType
from .generation_task_parameters import GenerationTaskParameters

from typing import Union, Tuple, List, Optional, Iterable, Type, Dict


class DataNotYetAvailable(Exception):
    pass


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

    def get_latest_version(self) -> "AssetVersion":
        version_data = self._get_data_provider().get_asset_version_data(self.path_id, None)
        return self.get_version(version_data.version_id)

    def get_default_version(self) -> "AssetVersion":
        # default version for an asset may be locked in the environment
        mapping = json.loads(os.environ.get('LBATTR_locked_asset_versions', '{}'))

        asset_ver_pathid = mapping.get(self.path_id)
        if asset_ver_pathid:
            # TODO: sanity check that mapped version belongs to this asset
            return self._get_version_class().from_path_id(self._get_data_provider(), asset_ver_pathid)
        return self.get_latest_version()

    def get_version(self, version_id: VersionType) -> "AssetVersion":
        version_id = normalize_version(version_id)

        return self._get_version_class()(self, version_id)

    def create_new_generic_version(self, version_id: Optional[VersionType] = None,
                                   creation_task_parameters: Optional[GenerationTaskParameters] = None,
                                   dependencies: Iterable["AssetVersion"] = (),
                                   create_template_from_locks: bool = False) -> Tuple["AssetVersion", List["AssetVersion"]]:
        """
        :returns: newly created asset version, and ALL other asset versions whos creation was triggered by that version
        """
        if version_id is not None:
            version_id = normalize_version(version_id)
        version_data = AssetVersionData(None,
                                        self.path_id,
                                        version_id,
                                        creation_task_parameters or GenerationTaskParameters({}, {}, {}),
                                        DataState.NOT_COMPUTED,
                                        None,
                                        None)
        version_data = self._get_data_provider().publish_new_asset_version(self.path_id, version_data, [dep.path_id for dep in dependencies])
        new_version = self._get_version_class()(self, version_data.version_id)
        triggered_versions = self._trigger_relevant_asset_templates(new_version)
        if create_template_from_locks and creation_task_parameters:
            # split dependencies into dynamic and static ones based on the lock dict
            locks = creation_task_parameters.version_lock_mapping
            if len(locks) > 0:  # template only makes sense if there are dynamic versions
                # only deps that are not part of the lock, static deps
                template_version_deps = [x.path_id for x in dependencies if x.path_id not in set(locks.values())]
                trigger_asset_pathids = list(locks.keys())
                self._get_data_provider().create_asset_template(AssetTemplateData(self.path_id, creation_task_parameters),
                                                                trigger_asset_pathids,
                                                                template_version_deps)
        return new_version, triggered_versions

    def _trigger_relevant_asset_templates(self, asset_version: "AssetVersion") -> List["AssetVersion"]:
        """
        trigger creation of new versions from all relevant templates for which asset_path_id is input.
        should recursively call itself on all version it itself creates too
        """
        # TODO: this assumes dependencies are a tree, instead implement an arbitrary graph
        asset = asset_version.asset
        result = []
        for template_data in self._get_data_provider().get_asset_templates_triggered_by(asset.path_id):
            # first of all - update template_data from DB.
            # due to strange recursion here - it is possible for template data to become outdated
            # TODO: maybe implement generator for get_asset_templates_triggered_by ?
            template_data = self._get_data_provider().get_asset_template_data_for_asset_path_id(template_data.asset_path_id)

            triggered_asset = Asset(template_data.asset_path_id, self._get_data_provider())
            data_producer_attrs = template_data.data_producer_task_attrs
            # data_producer_attrs.version_lock_mapping = data_producer_attrs.version_lock_mapping.copy()

            data_producer_attrs.version_lock_mapping[asset.path_id] = asset_version.path_id

            fixed_dependencies = [AssetVersion.from_path_id(self._get_data_provider(), x) for x in self._get_data_provider().get_template_fixed_dependencies(template_data.asset_path_id)]
            dependencies = [*fixed_dependencies,
                            *(AssetVersion.from_path_id(self._get_data_provider(), x) for x in data_producer_attrs.version_lock_mapping.values())]
            self._get_data_provider().update_asset_template_data(template_data)
            new_version, triggered_versions = triggered_asset.create_new_generic_version(None, data_producer_attrs, dependencies)
            result.extend([new_version, *triggered_versions])
        return result

    def _get_data_provider(self) -> DataAccessInterface:
        return self.__data_provider

    @classmethod
    def _get_version_class(cls):
        return AssetVersion

    @classmethod
    def type_name(cls):
        return cls.__name__

    def __hash__(self):
        return hash(self.path_id)


class AssetVersion:
    def __init__(self, asset: Asset, version_id: VersionType):
        self.__asset = asset
        self.__version_id = normalize_version(version_id)
        self._fresh_asset_version_data()  # this will raise if asset+version_id are invalid

    @classmethod
    def from_path_id(cls, data_provider: DataAccessInterface, version_path_id: str) -> "AssetVersion":
        data = data_provider.get_asset_version_data_from_path_id(version_path_id)
        return cls(Asset(data.asset_path_id, data_provider), data.version_id)

    @property
    def version_id(self) -> VersionType:
        return denormalize_version(self.__version_id)

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
        if data.data_availability == DataState.AVAILABLE:
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

    # TODO: all dependencies are SUPPOSED to return instances of proper classes, not of this one
    #  therefore they need to know director, but i don't want this class to depend on director
    #  so hmmmmmm...
    def get_dependencies(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        return [AssetVersion.from_path_id(self.data_provider, x) for x in self.data_provider.get_version_dependencies(data.path_id)]

    def get_dependants(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        return [AssetVersion.from_path_id(self.data_provider, x) for x in self.data_provider.get_dependent_versions(data.path_id)]

    def add_dependencies(self, dependencies: Iterable["AssetVersion"]):
        self.data_provider.add_dependencies(self.path_id, (dep.path_id for dep in dependencies))

    def __hash__(self):
        return hash(self.path_id)
