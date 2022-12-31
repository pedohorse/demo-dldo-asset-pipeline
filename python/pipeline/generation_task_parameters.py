from dataclasses import dataclass
import json

from typing import Dict, Any


@dataclass
class EnvironmentResolverParameters:
    name: str
    attribs: Dict[str, Any]


@dataclass
class GenerationTaskParameters:
    version_lock_mapping: Dict[str, str]
    attributes: Dict[str, Any]
    environment_arguments: EnvironmentResolverParameters

    def serialize(self) -> str:
        return json.dumps({'lock': self.version_lock_mapping,
                           'attrib': self.attributes,
                           'env': {'name': self.environment_arguments.name,
                                   'attribs': self.environment_arguments.attribs}})

    @classmethod
    def deserialize(cls, data: str) -> "GenerationTaskParameters":
        raw = json.loads(data)
        env = raw.get('env', {})
        return GenerationTaskParameters(raw.get('lock', {}),
                                        raw.get('attrib', {}),
                                        EnvironmentResolverParameters(env['name'], env['attribs']))
