import os
from pathlib import Path
from getpass import getuser
from hashlib import md5
from shutil import copy2
from pipeline.asset import Asset, AssetVersion, VersionType
from pipeline.generation_task_parameters import GenerationTaskParameters, EnvironmentResolverParameters
from pipeline.specialized_asset_base import SpecializedAssetBase, SpecializedAssetVersionBase

from typing import Optional, Tuple, Iterable, Dict, List


class SourcedAssetCommon(SpecializedAssetBase):
    def store_source(self, path: Path) -> Path:
        # save scene into some immutable location.
        with open(path, 'rb') as f:
            shash = md5(f.read())
        # we don't append filename as we rely on hip masking feature
        source_path = Path(self._get_data_provider().get_pipeline_source_root()) / shash.hexdigest() / path.name
        if not source_path.exists():
            source_path.parent.mkdir(parents=True, exist_ok=True)
            copy2(path, source_path)
        return source_path


class HipSourcedAssetCommon(SourcedAssetCommon):
    def generate_lifeblood_attributes(self, source_hip, driver_node_path, *,
                                      compute_type_name, task_name, mask_as_hip, frame_range, lock_asset_versions, extra_env_requirements):
        base_env_requirements = {'user': getuser()}  # theoretically all packages should be retrieved here from env, but we have a simplified example
        if extra_env_requirements:
            base_env_requirements.update(extra_env_requirements)

        creation_task_attributes = \
            {'name': task_name,
             'attribs': {'eat': 'shit',
                         'data_compute_type': compute_type_name,
                         'hipfile': str(source_hip),
                         'hiporig': str(mask_as_hip),
                         'hipdriver': driver_node_path,
                         'frames': list(range(int(frame_range[0]), int(frame_range[1]) + 1)),
                         'requirements': {'cpu': {'min': 2, 'pref': 32},
                                          'cmem': {'min': 4, 'pref': 16}},
                         },
             }
        generation_task_parameters = GenerationTaskParameters(lock_asset_versions or {},
                                                              creation_task_attributes,
                                                              EnvironmentResolverParameters('StandardEnvironmentResolver',  # we assume a single environment resolver is defined for the whole pipeline
                                                                                            base_env_requirements)
                                                              )
        return generation_task_parameters


class CacheAsset(HipSourcedAssetCommon):
    """
    this asset represents something with cache, for ex: model, geometry sequence, vdb sequence
    """
    def create_new_version(self, source: Tuple[str, str], frame_range: Tuple[int, int], is_sim: bool = False, version_id: Optional[VersionType] = None,
                           *, extra_env_requirements: Optional[Dict[str, str]] = None,
                           lock_asset_versions: Dict[str, str] = None,
                           dependencies: Iterable["AssetVersion"] = (),
                           create_template_from_locks: bool = False) -> Tuple["AssetVersion", List["AssetVersion"]]:
        """

        :param source: hip file path, and rop node path that generate final cache
        :param version_id:
        :param frame_range:
        :param is_sim:
        :param extra_env_requirements:
        :param lock_asset_versions:
        :param dependencies:
        :param create_template_from_locks:
        :return:
        """
        source_path = self.store_source(Path(source[0]))

        generation_task_parameters = self.generate_lifeblood_attributes(
            source_path, source[1],
            compute_type_name='cache',
            task_name=f'{self.path_id}, ver {version_id}: data creation',
            mask_as_hip=source[0],
            frame_range=frame_range,
            lock_asset_versions=lock_asset_versions,
            extra_env_requirements=extra_env_requirements
        )

        if is_sim:
            generation_task_parameters.attributes['attribs']['framechunk_size'] = frame_range[1] - frame_range[0] + 1

        return self.create_new_generic_version(version_id,
                                               creation_task_parameters=generation_task_parameters,
                                               dependencies=dependencies,
                                               create_template_from_locks=create_template_from_locks)

    @classmethod
    def _get_version_class(cls):
        return CacheAssetVersion


class CacheAssetVersion(SpecializedAssetVersionBase):
    @property
    def cache_path(self):
        return self.get_data()['cache_path_template']

    @property
    def frame_range(self):
        return self.get_data()['frame_range']


class RenderAsset(HipSourcedAssetCommon):
    """
    this asset represents something rendered, image sequence
    """
    def create_new_version(self, source: Tuple[str, str], frame_range: Tuple[int, int], version_id: Optional[VersionType] = None,
                           *, extra_env_requirements: Optional[Dict[str, str]] = None,
                           lock_asset_versions: Dict[str, str],
                           dependencies: Iterable["AssetVersion"] = (),
                           create_template_from_locks: bool = False) -> Tuple["AssetVersion", List["AssetVersion"]]:
        source_path = self.store_source(Path(source[0]))

        generation_task_parameters = self.generate_lifeblood_attributes(
            source_path, source[1],
            compute_type_name='render',
            task_name=f'{self.path_id}, ver {version_id}: data creation',
            mask_as_hip=source[0],
            frame_range=frame_range,
            lock_asset_versions=lock_asset_versions,
            extra_env_requirements=extra_env_requirements
        )

        generation_task_parameters.attributes['attribs']['requirements_render'] = \
            {'cpu': {'min': 2, 'pref': 8},
             'cmem': {'min': 2, 'pref': 6}}

        return self.create_new_generic_version(version_id,
                                               creation_task_parameters=generation_task_parameters,
                                               dependencies=dependencies,
                                               create_template_from_locks=create_template_from_locks)

    @classmethod
    def _get_version_class(cls):
        return RenderAssetVersion


class RenderAssetVersion(SpecializedAssetVersionBase):
    def render_sequence_path(self):
        return self.get_data()['render_path_template']

    def frame_range(self):
        return self.get_data()['frame_range']


class ComposeAsset(SourcedAssetCommon):
    def generate_lifeblood_attributes(self, source_file, *,
                                      compute_type_name, task_name, frame_range, lock_asset_versions, extra_env_requirements):
        base_env_requirements = {'user': getuser()}  # theoretically all packages should be retrieved here from env, but we have a simplified example
        if extra_env_requirements:
            base_env_requirements.update(extra_env_requirements)

        creation_task_attributes = \
            {'name': task_name,
             'attribs': {'eat': 'shit',
                         'data_compute_type': compute_type_name,
                         'file': str(source_file),
                         'frames': list(range(int(frame_range[0]), int(frame_range[1]) + 1)),
                         'requirements': {'cpu': {'min': 2, 'pref': 32},
                                          'cmem': {'min': 4, 'pref': 16}},
                         },
             }
        generation_task_parameters = GenerationTaskParameters(lock_asset_versions or {},
                                                              creation_task_attributes,
                                                              EnvironmentResolverParameters('StandardEnvironmentResolver',  # we assume a single environment resolver is defined for the whole pipeline
                                                                                            base_env_requirements)
                                                              )
        return generation_task_parameters

    def create_new_version(self, source: str, frame_range: Tuple[int, int], version_id: Optional[VersionType] = None,
                           *, extra_env_requirements: Optional[Dict[str, str]] = None,
                           lock_asset_versions: Dict[str, str],
                           dependencies: Iterable["AssetVersion"] = (),
                           create_template_from_locks: bool = False) -> Tuple["AssetVersion", List["AssetVersion"]]:
        source_path = self.store_source(Path(source))

        generation_task_parameters = self.generate_lifeblood_attributes(
            source_path,
            compute_type_name='blender_render',
            task_name=f'comp task for {self.name}',
            frame_range=frame_range,
            lock_asset_versions=lock_asset_versions,
            extra_env_requirements=extra_env_requirements
        )

        generation_task_parameters.attributes['attribs']['requirements_render'] = \
            {'cpu': {'min': 2, 'pref': 8},
             'cmem': {'min': 2, 'pref': 6}}

        return self.create_new_generic_version(version_id,
                                               creation_task_parameters=generation_task_parameters,
                                               dependencies=dependencies,
                                               create_template_from_locks=create_template_from_locks)

    @classmethod
    def _get_version_class(cls):
        return ComposeAssetVersion


class ComposeAssetVersion(SpecializedAssetVersionBase):
    def render_sequence_path(self):
        return self.get_data()['render_path_template']

    def frame_range(self):
        return self.get_data()['frame_range']
