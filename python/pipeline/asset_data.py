import json
from enum import Enum
from dataclasses import dataclass
from .generation_task_parameters import GenerationTaskParameters

from typing import List, Optional, Tuple


class DataState(Enum):
    NOT_COMPUTED = 0
    IS_COMPUTING = 1
    AVAILABLE = 2


@dataclass
class AssetData:
    path_id: str
    name: str
    description: str


@dataclass
class AssetVersionData:
    path_id: str
    asset_path_id: str
    version_id: Tuple[int, int, int]
    data_producer_task_attrs: GenerationTaskParameters
    data_availability: DataState
    data_calculator_id: str
    data: Optional[dict]


@dataclass
class AssetTemplateData:
    asset_path_id: str
    data_producer_task_attrs: GenerationTaskParameters

    def __hash__(self):
        return hash(self.asset_path_id)
