from typing import Iterable

from lifeblood.basenode import BaseNode, ProcessingResult, ProcessingContext, ProcessingError, NodeParameterType
from lifeblood.taskspawn import TaskSpawn
from lifeblood.invocationjob import InvocationJob
from demo_pipeline import get_director


def node_class():
    return AssetDependencyChecker


class AssetDependencyChecker(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'Asset Dependency Checker'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'pipeline', 'asset'

    @classmethod
    def type_name(cls) -> str:
        return 'pipeline_AssetDependencyChecker'

    def __init__(self, name):
        super(AssetDependencyChecker, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('asset_path_id to get deps for', 'Asset Version id', NodeParameterType.STRING, '`task["asset_version_id"]`')
            ui.add_parameter('with no data only', 'create tasks for versions with data not computed', NodeParameterType.BOOL, True)
            ui.add_separator()
            data_task_param = ui.add_parameter('create data task', 'create data task', NodeParameterType.BOOL, False)
            ui.add_parameter('inherit attribs', 'inherit attributes', NodeParameterType.BOOL, True).append_visibility_condition(data_task_param, 'eq', False)

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        asset_version_pid = context.param_value('asset_path_id to get deps for')
        only_nodata = context.param_value('with no data only')
        create_data_task = context.param_value('create data task')
        base_attribs = context.task_attributes() if context.param_value('inherit attribs') else {}

        director = get_director()
        version = director.get_asset_version(asset_version_pid)
        deps = version.get_dependencies()
        if only_nodata:
            deps = [x for x in deps if not x.is_data_available()]

        spawns = []
        if create_data_task:
            script = f'for dep_id in {repr([x.path_id for x in deps])}:\n' \
                     f'    from demo_pipeline import get_director\n' \
                     f'    get_director().get_asset_version(dep_id).schedule_data_calculation_if_needed()\n'
            inv = InvocationJob(['python', ':/script.py'])
            inv.set_extra_file('script.py', script)
            return ProcessingResult(inv)
        else:
            for dep in deps:
                attribs = dict(base_attribs)
                attribs['asset_version_id'] = dep.path_id
                attribs['asset_version_version'] = dep.version_id
                spawns.append(TaskSpawn(f'asset {dep.asset.name}, version: {".".join(dep.version_id)}',
                                        task_attributes=attribs))

            return ProcessingResult(spawn=spawns)
