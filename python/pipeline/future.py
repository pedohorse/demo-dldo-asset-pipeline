import time

from typing import Callable, Any


class FutureResult:
    def is_result_ready(self) -> bool:
        raise NotImplementedError()

    def wait_for_result(self):
        raise NotImplementedError()


class CompletedFuture(FutureResult):
    def __init__(self, value):
        self.__val = value

    def is_result_ready(self) -> bool:
        return True

    def wait_for_result(self):
        return self.__val


class ConditionCheckerFuture(FutureResult):
    def __init__(self, condition: Callable[[], bool], result_getter: Callable[[], Any], poll_time=0.1):
        self.__condition = condition
        self.__result_getter = result_getter
        self.__poll_time = poll_time

    def is_result_ready(self) -> bool:
        return self.__condition()

    def wait_for_result(self):
        while not self.__condition():
            time.sleep(self.__poll_time)
        return self.__result_getter()
