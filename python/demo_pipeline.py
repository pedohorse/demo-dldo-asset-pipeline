from pipeline.director import Director
from pipeline_impl.sqlite_data_manager import SqliteDataManagerWithLifeblood
from pipeline_impl.uri_multihandler import UriMultiHandler
from pipeline_impl.asset_uri_handler import AssetUriHandler

lb_addr = ('192.168.0.28', 1384)
__dm = SqliteDataManagerWithLifeblood('/tmp/smth.db', lb_addr)
__director: Director = Director(__dm,
                                UriMultiHandler([AssetUriHandler(__dm)]))


def get_director() -> Director:
    assert __director is not None
    return __director
