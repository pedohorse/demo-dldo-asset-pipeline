from .asset_data import AssetVersionData
from .generation_task_parameters import GenerationTaskParameters
from .future import FutureResult

from typing import Tuple


class TaskSchedulingResultReportReceiver:
    def data_computation_completed_callback(self, path_id: str, data: dict):
        raise NotImplementedError()


class TaskSchedulingInterface:
    def __init__(self):
        self.__task_completion_report_receivers = []

    def add_task_completion_callback_receiver(self, callback_receiver: TaskSchedulingResultReportReceiver):
        if callback_receiver not in self.__task_completion_report_receivers:
            self.__task_completion_report_receivers.append(callback_receiver)

    def get_task_completion_receivers(self) -> Tuple[TaskSchedulingResultReportReceiver, ...]:
        return tuple(self.__task_completion_report_receivers)

    def schedule_data_generation_task(self, asset_version_data: AssetVersionData, task_data_generation_data: GenerationTaskParameters) -> (FutureResult, str):
        """
        schedules data generation task for asset_version_data according to task_data_generation_data.

        Returns:
            FutureResult, str: future that one can wait for the result with and some ID that can be used to identify this scheduling event.
                               Note: this id can, but does not have to do anything with underlying internal jobids or anything
                                     so nothing should treat it as anything but event id for this TaskSchedulingInterface
        """
        raise NotImplementedError()

    def get_schedule_event_future(self, event_id: str) -> FutureResult:
        """
        given event_id as returned from schedule_data_generation_task returns a future, associated with it
        """
        raise NotImplementedError()
