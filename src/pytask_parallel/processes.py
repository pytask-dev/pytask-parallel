"""Contains functions related to processes and loky."""

from __future__ import annotations

import inspect
import sys
import warnings
from functools import partial
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

import cloudpickle
from pytask import Mark
from pytask import PTask
from pytask import PythonNode
from pytask import Session
from pytask import WarningReport
from pytask import console
from pytask import get_marks
from pytask import hookimpl
from pytask import parse_warning_filter
from pytask import remove_internal_traceback_frames_from_exc_info
from pytask import warning_record_to_str
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map
from rich.traceback import Traceback

from pytask_parallel.utils import create_kwargs_for_task
from pytask_parallel.utils import handle_task_function_return

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path
    from types import ModuleType
    from types import TracebackType

    from rich.console import ConsoleOptions


@hookimpl
def pytask_execute_task(session: Session, task: PTask) -> Future[Any] | None:
    """Execute a task.

    Take a task, pickle it and send the bytes over to another process.

    """
    if session.config["n_workers"] > 1:
        kwargs = create_kwargs_for_task(task)

        # Task modules are dynamically loaded and added to `sys.modules`. Thus,
        # cloudpickle believes the module of the task function is also importable in
        # the child process. We have to register the module as dynamic again, so
        # that cloudpickle will pickle it with the function. See cloudpickle#417,
        # pytask#373 and pytask#374.
        task_module = _get_module(task.function, getattr(task, "path", None))
        cloudpickle.register_pickle_by_value(task_module)

        return session.config["_parallel_executor"].submit(
            _execute_task,
            task=task,
            kwargs=kwargs,
            show_locals=session.config["show_locals"],
            console_options=console.options,
            session_filterwarnings=session.config["filterwarnings"],
            task_filterwarnings=get_marks(task, "filterwarnings"),
        )
    return None


def _raise_exception_on_breakpoint(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
    msg = (
        "You cannot use 'breakpoint()' or 'pdb.set_trace()' while parallelizing the "
        "execution of tasks with pytask-parallel. Please, remove the breakpoint or run "
        "the task without parallelization to debug it."
    )
    raise RuntimeError(msg)


def _patch_set_trace_and_breakpoint() -> None:
    """Patch :func:`pdb.set_trace` and :func:`breakpoint`.

    Patch sys.breakpointhook to intercept any call of breakpoint() and pdb.set_trace in
    a subprocess and print a better exception message.

    """
    import pdb  # noqa: T100
    import sys

    pdb.set_trace = _raise_exception_on_breakpoint
    sys.breakpointhook = _raise_exception_on_breakpoint


def _execute_task(  # noqa: PLR0913
    task: PTask,
    kwargs: dict[str, Any],
    show_locals: bool,  # noqa: FBT001
    console_options: ConsoleOptions,
    session_filterwarnings: tuple[str, ...],
    task_filterwarnings: tuple[Mark, ...],
) -> tuple[
    PyTree[PythonNode | None],
    list[WarningReport],
    tuple[type[BaseException], BaseException, str] | None,
]:
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute in a
    spawned process or thread.

    """
    __tracebackhide__ = True
    _patch_set_trace_and_breakpoint()

    with warnings.catch_warnings(record=True) as log:
        for arg in session_filterwarnings:
            warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        # apply filters from "filterwarnings" marks
        for mark in task_filterwarnings:
            for arg in mark.args:
                warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        try:
            out = task.execute(**kwargs)
        except Exception:  # noqa: BLE001
            exc_info = sys.exc_info()
            processed_exc_info = _process_exception(
                exc_info, show_locals, console_options
            )
        else:
            handle_task_function_return(task, out)
            processed_exc_info = None

        task_display_name = getattr(task, "display_name", task.name)
        warning_reports = []
        for warning_message in log:
            fs_location = warning_message.filename, warning_message.lineno
            warning_reports.append(
                WarningReport(
                    message=warning_record_to_str(warning_message),
                    fs_location=fs_location,
                    id_=task_display_name,
                )
            )

    python_nodes = tree_map(
        lambda x: x if isinstance(x, PythonNode) else None, task.produces
    )

    return python_nodes, warning_reports, processed_exc_info


def _process_exception(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None],
    show_locals: bool,  # noqa: FBT001
    console_options: ConsoleOptions,
) -> tuple[type[BaseException], BaseException, str]:
    """Process the exception and convert the traceback to a string."""
    exc_info = remove_internal_traceback_frames_from_exc_info(exc_info)
    traceback = Traceback.from_exception(*exc_info, show_locals=show_locals)
    segments = console.render(traceback, options=console_options)
    text = "".join(segment.text for segment in segments)
    return (*exc_info[:2], text)


def _get_module(func: Callable[..., Any], path: Path | None) -> ModuleType:
    """Get the module of a python function.

    ``functools.partial`` obfuscates the module of the function and
    ``inspect.getmodule`` returns :mod`functools`. Therefore, we recover the original
    function.

    We use the path from the task module to aid the search although it is not clear
    whether it helps.

    """
    if isinstance(func, partial):
        func = func.func

    if path:
        return inspect.getmodule(func, path.as_posix())
    return inspect.getmodule(func)
