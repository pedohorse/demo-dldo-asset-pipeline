from getpass import getuser
from pipeline.asset import Asset, AssetVersion, VersionType

from typing import Optional, Tuple, Iterable, Dict


class CacheAsset(Asset):
    """
    this asset represents something with cache, for ex: model, geometry sequence, vdb sequence
    """
    def create_new_version(self, source: Tuple[str, str], frame_range: Tuple[int, int], is_sim: bool = False, version_id: Optional[VersionType] = None,
                           *, extra_env_requirements: Optional[Dict[str, str]] = None,
                           lock_asset_versions: Dict[str, str] = None,
                           dependencies: Iterable["AssetVersion"] = ()) -> AssetVersion:
        """

        :param source: hip file path, and rop node path that generate final cache
        :param version_id:
        :param frame_range:
        :param is_sim:
        :param extra_env_requirements:
        :param lock_asset_versions:
        :param dependencies:
        :return:
        """
        base_env_requirements = {'user': getuser()}  # theoretically all packages should be retrieved here from env, but we have a simplified example
        if extra_env_requirements:
            base_env_requirements.update(extra_env_requirements)

        if lock_asset_versions is None:
            lock_asset_versions = {}

        creation_task_parameters = \
            {'name': f'{self.path_id}, ver {version_id}: data creation',
             'attribs': {'eat': 'shit',
                         'data_compute_type': 'cache',
                         'hipfile': source[0],
                         'hipdriver': source[1],
                         'frames': list(range(frame_range[0], frame_range[1]+1)),
                         'requirements': {'cpu': {'min': 2, 'pref': 32},
                                          'cmem': {'min': 4, 'pref': 16}},
                         'locked_asset_versions': lock_asset_versions
                         },
             'env': {'name': 'StandardEnvironmentResolver',  # we assume a single environment resolver is defined for the whole pipeline
                     'attribs': base_env_requirements
                     }
             }
        if is_sim:
            creation_task_parameters['attribs']['framechunk_size'] = frame_range[1] - frame_range[0] + 1

        return self.create_new_generic_version(version_id, creation_task_parameters, dependencies)

    @classmethod
    def _get_version_class(cls):
        return CacheAssetVersion


class CacheAssetVersion(AssetVersion):
    @property
    def cache_path(self):
        return self.get_data()['cache_path_template']

    @property
    def frame_range(self):
        return self.get_data()['frame_range']


class RenderAsset(Asset):
    """
    this asset represents something rendered, image sequence
    """
    def create_new_version(self, source: Tuple[str, str], frame_range: Tuple[int, int], version_id: Optional[VersionType] = None,
                           *, extra_env_requirements: Optional[Dict[str, str]] = None,
                           lock_asset_versions: Dict[str, str],
                           dependencies: Iterable["AssetVersion"] = ()) -> AssetVersion:

        base_env_requirements = {'user': getuser()}  # theoretically all packages should be retrieved here from env, but we have a simplified example
        if extra_env_requirements:
            base_env_requirements.update(extra_env_requirements)

        if lock_asset_versions is None:
            lock_asset_versions = {}

        creation_task_parameters = \
            {'name': f'{self.path_id}, ver {version_id}: data creation',
             'attribs': {'eat': 'shit',
                         'data_compute_type': 'cache',
                         'hipfile': source[0],
                         'hipdriver': source[1],
                         'frames': list(range(frame_range[0], frame_range[1] + 1)),
                         'requirements': {'cpu': {'min': 2, 'pref': 32},
                                          'cmem': {'min': 4, 'pref': 16}},
                         'locked_asset_versions': lock_asset_versions
                         },
             'env': {'name': 'StandardEnvironmentResolver',  # we assume a single environment resolver is defined for the whole pipeline
                     'attribs': base_env_requirements
                     }
             }

        return self.create_new_generic_version(version_id, creation_task_parameters, dependencies)

    @classmethod
    def _get_version_class(cls):
        return RenderAssetVersion


class RenderAssetVersion(AssetVersion):
    def render_sequence_path(self):
        return self.get_data()['cache_path_template']

    def frame_range(self):
        return self.get_data()['frame_range']
