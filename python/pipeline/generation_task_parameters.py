from dataclasses import dataclass
import json

from typing import Dict, Any


@dataclass
class GenerationTaskParameters:
    version_lock_mapping: Dict[str, str]
    attributes: Dict[str, Any]
    environment_arguments: Dict[str, Any]

    def serialize(self) -> str:
        return json.dumps({'lock': self.version_lock_mapping,
                           'attrib': self.attributes,
                           'env': self.environment_arguments})

    @classmethod
    def deserialize(cls, data: str) -> "GenerationTaskParameters":
        raw = json.loads(data)
        return GenerationTaskParameters(raw.get('lock', {}),
                                        raw.get('attrib', {}),
                                        raw.get('env', {}))
