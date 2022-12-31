from pipeline.asset import Asset, AssetVersion
from pipeline.generation_task_parameters import GenerationTaskParameters, EnvironmentResolverParameters
from pipeline_impl.specialized_assets import ComposeAsset
from demo_pipeline import get_director, NotFoundError
from typing import Iterable, Dict, Any, Optional, Tuple


def publish_version(asset_pathid: str,
                    dynamic_dependencies: Iterable[str],
                    fixed_dependencies: Iterable[str],
                    creation_task_attributes: Dict[str, Any],
                    env_args: EnvironmentResolverParameters,
                    create_template: bool):
    d = get_director()
    asset = d.get_asset(asset_pathid)

    dependencies = set(dynamic_dependencies)
    dependencies.update(fixed_dependencies)

    params = GenerationTaskParameters({d.get_asset_version(vpid).asset.path_id: vpid for vpid in dynamic_dependencies},
                                      creation_task_attributes,
                                      env_args)
    new_ver, _ = asset.create_new_generic_version(None,
                                                  creation_task_parameters=params,
                                                  dependencies=[d.get_asset_version(x) for x in dependencies],
                                                  create_template_from_locks=create_template)

    return new_ver


def publish_comp_version(asset_pathid: str, source: str, frame_range: Tuple[int, int],
                         *, extra_env_requirements: Optional[Dict[str, str]] = None,
                         dynamic_dependencies: Iterable[str],
                         fixed_dependencies: Iterable[str],
                         create_template_from_locks: bool = False):
    d = get_director()
    ass = d.get_asset(asset_pathid)

    assert isinstance(ass, ComposeAsset), f'{asset_pathid} is not a Compose asset'

    dependencies = set(dynamic_dependencies)
    dependencies.update(fixed_dependencies)

    lock_asset_versions = {d.get_asset_version(vpid).asset.path_id: vpid for vpid in dynamic_dependencies}

    return ass.create_new_version(source,
                                  frame_range=frame_range,
                                  extra_env_requirements=extra_env_requirements,
                                  lock_asset_versions=lock_asset_versions,
                                  dependencies=[d.get_asset_version(x) for x in dependencies],
                                  create_template_from_locks=create_template_from_locks)


def create_comp_asset(asset_pathid: str, name: str, description: str) -> str:
    d = get_director()

    try:
        d.get_asset(asset_pathid)
    except NotFoundError:
        pass
    else:
        raise RuntimeError(f'asset "{asset_pathid}" already exists')

    return d.new_comp_asset(name, description, asset_pathid).path_id
