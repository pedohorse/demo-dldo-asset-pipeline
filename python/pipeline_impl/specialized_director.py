from pipeline.director import Director, AssetFactory
from pipeline.specialized_asset_base import SpecializedAssetBase, Asset
from .specialized_assets import CacheAsset, RenderAsset

from typing import Type


class SpecializedAssetFactory(AssetFactory):
    def __init__(self, specialized_class: Type[SpecializedAssetBase], director: Director):
        self.__director = director
        self.__class = specialized_class

    def __call__(self, asset_path_id: str) -> Asset:
        return self.__class(asset_path_id, self.__director)

    def asset_type(self) -> Type[Asset]:
        return self.__class


class PipelineDirector(Director):
    def new_cache_asset(self, name: str, description: str, path_id: str) -> CacheAsset:
        new_asset = self.new_asset(name, description, CacheAsset.type_name(), path_id)
        assert isinstance(new_asset, CacheAsset), 'inconsistency!'
        return new_asset

    def new_render_asset(self, name: str, description: str, path_id: str) -> RenderAsset:
        new_asset = self.new_asset(name, description, RenderAsset.type_name(), path_id)
        assert isinstance(new_asset, RenderAsset), 'inconsistency!'
        return new_asset
