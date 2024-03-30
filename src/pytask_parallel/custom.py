"""Contains functions for the threads backend."""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from pytask import PTask
from pytask import Session
from pytask import hookimpl

from pytask_parallel.utils import create_kwargs_for_task

if TYPE_CHECKING:
    from concurrent.futures import Future


@hookimpl
def pytask_execute_task(session: Session, task: PTask) -> Future[Any]:
    """Execute a task.

    Since threads have shared memory, it is not necessary to pickle and unpickle the
    task.

    """
    kwargs = create_kwargs_for_task(task)
    return session.config["_parallel_executor"].submit(task.function, **kwargs)
