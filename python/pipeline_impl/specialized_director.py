from pipeline.director import Director, AssetFactory, DataAccessInterface, UriHandlerBase
from pipeline.specialized_asset_base import SpecializedAssetBase, Asset
from .specialized_assets import CacheAsset, RenderAsset, ComposeAsset

from typing import Iterable, Type


class SpecializedAssetFactory(AssetFactory):
    def __init__(self, specialized_class: Type[SpecializedAssetBase], director: Director):
        self.__director = director
        self.__class = specialized_class

    def __call__(self, asset_path_id: str) -> Asset:
        return self.__class(asset_path_id, self.__director)

    def asset_type(self) -> Type[Asset]:
        return self.__class


class PipelineDirector(Director):
    def __init__(self, data_accessor: DataAccessInterface, uri_handler: Iterable[UriHandlerBase] = ()):
        super().__init__(data_accessor, uri_handler)
        # register assets that we know we are using
        self.register_asset_type(SpecializedAssetFactory(CacheAsset, self))
        self.register_asset_type(SpecializedAssetFactory(RenderAsset, self))
        self.register_asset_type(SpecializedAssetFactory(ComposeAsset, self))

    def new_cache_asset(self, name: str, description: str, path_id: str) -> CacheAsset:
        new_asset = self.new_asset(name, description, CacheAsset.type_name(), path_id)
        assert isinstance(new_asset, CacheAsset), 'inconsistency!'
        return new_asset

    def new_render_asset(self, name: str, description: str, path_id: str) -> RenderAsset:
        new_asset = self.new_asset(name, description, RenderAsset.type_name(), path_id)
        assert isinstance(new_asset, RenderAsset), 'inconsistency!'
        return new_asset

    def new_comp_asset(self, name: str, description: str, path_id: str) -> ComposeAsset:
        new_asset = self.new_asset(name, description, ComposeAsset.type_name(), path_id)
        assert isinstance(new_asset, ComposeAsset), 'inconsistency!'
        return new_asset
