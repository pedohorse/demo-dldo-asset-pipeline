from typing import Iterable

from lifeblood.basenode import BaseNode, ProcessingResult, ProcessingContext, ProcessingError, NodeParameterType
from lifeblood.taskspawn import TaskSpawn
from lifeblood.invocationjob import InvocationJob
from demo_pipeline import get_director


def node_class():
    return AssetComputationFinalizer


class AssetComputationFinalizer(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'Asset Computation Finalizer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'pipeline', 'asset'

    @classmethod
    def type_name(cls) -> str:
        return 'pipeline_AssetComputationFinalizer'

    def __init__(self, name):
        super(AssetComputationFinalizer, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('asset_version_id', 'Asset Version id', NodeParameterType.STRING, '`task["asset_version_id"]`')
            ui.add_parameter('data attribute name', 'Data Attribute Name', NodeParameterType.STRING, 'data')

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        asset_version_pid = context.param_value('asset_version_id')
        data = context.task_attribute(context.param_value('data attribute name'))
        director = get_director()

        director.get_data_accessor()._data_computation_completed_callback(asset_version_pid, data)

        return ProcessingResult()
