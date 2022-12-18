from .asset import Asset, AssetVersion
from .director import Director, AssetFactory

from typing import List, Iterable


# TODO: instead of this maybe it's better to split a registry interface from director interface.
#  then we'd have tightly coupled Asset+AssetVersion+AssetRegistry, and director will only inherit them
class SpecializedAssetBase(Asset):
    def __init__(self, asset_path_id: str, director: Director):
        super(SpecializedAssetBase, self).__init__(asset_path_id, director.get_data_accessor())
        self.__director = director

    def _get_director(self) -> Director:
        return self.__director

    @classmethod
    def _get_version_class(cls):
        return SpecializedAssetVersionBase


class SpecializedAssetVersionBase(AssetVersion):
    @property
    def asset(self) -> SpecializedAssetBase:
        asset = super(SpecializedAssetVersionBase, self).asset
        assert isinstance(asset, SpecializedAssetBase)
        return asset

    def get_dependencies(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        director = self.asset._get_director()
        return [director.get_asset_version(x) for x in self.data_provider.get_version_dependencies(data.path_id)]

    def get_dependants(self) -> List["AssetVersion"]:
        data = self._fresh_asset_version_data()
        director = self.asset._get_director()
        return [director.get_asset_version(x) for x in self.data_provider.get_dependent_versions(data.path_id)]
