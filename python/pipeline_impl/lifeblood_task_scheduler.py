from pipeline.asset_data import AssetVersionData
from pipeline.utils import denormalize_version
from pipeline.task_scheduling_interface import TaskSchedulingInterface, TaskSchedulingResultReportReceiver
from pipeline.generation_task_parameters import GenerationTaskParameters
from pipeline.future import FutureResult, ConditionCheckerFuture

from lifeblood_client.query import Task
from lifeblood_client.submitting import NewTask, EnvironmentResolverArguments

from typing import Tuple

try:
    import lifeblood_connection
    in_lifeblood_runtime = True
except ImportError:
    in_lifeblood_runtime = False


class LifebloodTaskFuture(ConditionCheckerFuture):
    def __init__(self, addr: Tuple[str, int], task_id: str):
        task_id = int(task_id)
        self.__task = Task(addr, task_id)
        super(LifebloodTaskFuture, self).__init__(self._check,
                                                  self._get_result)

    def _check(self):
        task = self.__task
        return task.state == Task.TaskState.DONE and task.paused \
               or task.state == Task.TaskState.ERROR

    def _get_result(self):
        """
        just return True on success, False on Failure
        we expect lifeblood graph to be responsible for data setting to DB
        """
        return self._check() and self.__task.state != Task.TaskState.ERROR

    def get_lifeblood_task(self) -> Task:
        return self.__task


class LifebloodDataScheduler(TaskSchedulingInterface):
    def __init__(self, lifeblood_address: Tuple[str, int]):
        super().__init__()
        self.__lb_addr = lifeblood_address

    def schedule_data_generation_task(self, asset_version_data: AssetVersionData, task_data_generation_data: GenerationTaskParameters) -> (FutureResult, str):
        env_args = EnvironmentResolverArguments(task_data_generation_data.environment_arguments.name or 'StandardEnvironmentResolver',
                                                task_data_generation_data.environment_arguments.attribs)

        task_stuff = task_data_generation_data.attributes  # this contains lifeblood-formated stuff, maybe TODO: standardize, generalize, type
        task_stuff.setdefault('attribs', {})['asset_version_id'] = asset_version_data.path_id
        task_stuff['attribs']['asset_id'] = asset_version_data.asset_path_id
        task_stuff['attribs']['version'] = denormalize_version(asset_version_data.version_id)
        task_stuff['attribs']['locked_asset_versions'] = task_data_generation_data.version_lock_mapping
        # note that at this point task_data_generation_data.attributes are tainted, DON'T use it later here, or just copy it above
        if in_lifeblood_runtime:
            # TODO: this does not take env into account...
            task_id = lifeblood_connection.create_task(task_stuff['name'], task_stuff['attribs'], env_arguments=env_args, blocking=True)
        else:
            task = NewTask(name=task_stuff.get('name', 'just some unnamed task'),
                           node_id=task_stuff.get('node_id', 2),  # 2 here is what defined by lifeblood network setup, the node has id=2, just so happened
                           scheduler_addr=self.__lb_addr,
                           env_args=env_args,
                           task_attributes=task_stuff.get('attribs', {}),
                           priority=task_stuff.get('priority', 50)).submit()
            task_id = task.id

        task_id = str(task_id)
        return self.get_schedule_event_future(task_id), task_id

    def get_schedule_event_future(self, event_id: str) -> FutureResult:
        return LifebloodTaskFuture(self.__lb_addr, event_id)
