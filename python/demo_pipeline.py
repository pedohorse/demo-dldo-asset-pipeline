import os
from pipeline_impl.specialized_director import PipelineDirector, Director
from pipeline.data_access_interface import NotFoundError  # export
from pipeline_impl.sqlite_data_manager import SqliteDataManagerWithLifeblood
from pipeline_impl.asset_uri_handler import AssetUriHandler
from pipeline_impl.asset_version_uri_handler import AssetVersionUriHandler

from pipeline_impl.specialized_assets import CacheAsset, RenderAsset

lb_addr = ('192.168.0.28', 1384)
__dm = SqliteDataManagerWithLifeblood(os.path.join(os.environ['PIPELINE_ROOT'], 'smth.db'), lb_addr)
__director: PipelineDirector = PipelineDirector(__dm)
__director.register_uri_handler(AssetUriHandler(__director))
__director.register_uri_handler(AssetVersionUriHandler(__director))

__director.register_asset_type(CacheAsset)
__director.register_asset_type(RenderAsset)


def get_director() -> Director:
    assert __director is not None
    return __director
