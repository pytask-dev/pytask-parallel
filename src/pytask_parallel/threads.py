"""Contains functions for parallel execution of tasks with threads."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from typing import Any

from pytask import PTask
from pytask import Session
from pytask import Task
from pytask import hookimpl

from pytask_parallel.utils import create_kwargs_for_task
from pytask_parallel.utils import handle_task_function_return

if TYPE_CHECKING:
    from concurrent.futures import Future
    from types import TracebackType


@hookimpl
def pytask_execute_task(session: Session, task: Task) -> Future[Any]:
    """Execute a task.

    Since threads have shared memory, it is not necessary to pickle and unpickle the
    task.

    """
    kwargs = create_kwargs_for_task(task)
    return session.config["_parallel_executor"].submit(
        _mock_processes_for_threads, task=task, **kwargs
    )


def _mock_processes_for_threads(
    task: PTask, **kwargs: Any
) -> tuple[
    None, list[Any], tuple[type[BaseException], BaseException, TracebackType] | None
]:
    """Mock execution function such that it returns the same as for processes.

    The function for processes returns ``warning_reports`` and an ``exception``. With
    threads, these object are collected by the main and not the subprocess. So, we just
    return placeholders.

    """
    __tracebackhide__ = True
    try:
        out = task.function(**kwargs)
    except Exception:  # noqa: BLE001
        exc_info = sys.exc_info()
    else:
        handle_task_function_return(task, out)
        exc_info = None
    return None, [], exc_info
