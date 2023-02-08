import os
from pipeline_impl.specialized_director import SpecializedAssetFactory, PipelineDirector, Director
from pipeline.data_access_interface import NotFoundError, TaskSchedulerNotAvailable  # export
from pipeline_impl.sqlite_data_manager import SqliteDataManagerWithLifeblood
from pipeline_impl.lifeblood_task_scheduler import LifebloodDataScheduler
from pipeline_impl.asset_uri_handler import AssetUriHandler
from pipeline_impl.asset_version_uri_handler import AssetVersionUriHandler

from pipeline_impl.specialized_assets import CacheAsset, RenderAsset, ComposeAsset

lb_addr = ('127.0.0.1', 1384)
__scheduler = LifebloodDataScheduler(lb_addr)
__dm = SqliteDataManagerWithLifeblood(os.path.join(os.environ['PIPELINE_ROOT'], 'smth.db'), __scheduler)
__scheduler.add_task_completion_callback_receiver(__dm)
__director: PipelineDirector = PipelineDirector(__dm)
__director.register_uri_handler(AssetUriHandler(__director))
__director.register_uri_handler(AssetVersionUriHandler(__director))


def get_director() -> PipelineDirector:
    assert __director is not None
    return __director
