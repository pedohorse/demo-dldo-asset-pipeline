from typing import Union, Tuple

VersionType = Union[int, Tuple[int], Tuple[int, int], Tuple[int, int, int]]


def normalize_version(version_id: VersionType) -> Tuple[int, int, int]:
    if isinstance(version_id, int):
        return version_id, -1, -1
    if len(version_id) >= 3:
        return version_id[:3]
    return (*version_id, *((-1,)*(3-len(version_id))))


def denormalize_version(version_id: Tuple[int, int, int]) -> VersionType:
    if version_id[1] == -1:
        return version_id[0]
    if version_id[2] == -1:
        return version_id[:2]
    return version_id

