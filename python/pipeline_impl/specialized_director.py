from pipeline.director import Director
from .specialized_assets import CacheAsset, RenderAsset


class PipelineDirector(Director):
    def new_cache_asset(self, name: str, description: str, path_id: str) -> CacheAsset:
        new_asset = self.new_asset(name, description, CacheAsset.type_name(), path_id)
        assert isinstance(new_asset, CacheAsset), 'inconsistency!'
        return new_asset

    def new_render_asset(self, name: str, description: str, path_id: str) -> RenderAsset:
        new_asset = self.new_asset(name, description, RenderAsset.type_name(), path_id)
        assert isinstance(new_asset, RenderAsset), 'inconsistency!'
        return new_asset
