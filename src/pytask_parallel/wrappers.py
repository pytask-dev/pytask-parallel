"""Contains functions related to processes and loky."""

from __future__ import annotations

import sys
import warnings
from contextlib import redirect_stderr
from contextlib import redirect_stdout
from io import StringIO
from typing import TYPE_CHECKING
from typing import Any

from attrs import define
from pytask import PTask
from pytask import PythonNode
from pytask import Traceback
from pytask import WarningReport
from pytask import console
from pytask import parse_warning_filter
from pytask import warning_record_to_str
from pytask.tree_util import PyTree
from pytask.tree_util import tree_leaves
from pytask.tree_util import tree_map
from pytask.tree_util import tree_structure

if TYPE_CHECKING:
    from types import TracebackType

    from pytask import Mark
    from rich.console import ConsoleOptions


__all__ = ["wrap_task_in_process", "wrap_task_in_thread"]


@define(kw_only=True)
class WrapperResult:
    python_nodes: PyTree[PythonNode | None]
    warning_reports: list[WarningReport]
    exc_info: tuple[type[BaseException], BaseException, TracebackType | str] | None
    stdout: str
    stderr: str


def _handle_task_function_return(task: PTask, out: Any) -> None:
    """Handle the return value of a task function."""
    if "return" not in task.produces:
        return

    structure_out = tree_structure(out)
    structure_return = tree_structure(task.produces["return"])
    # strict must be false when none is leaf.
    if not structure_return.is_prefix(structure_out, strict=False):
        msg = (
            "The structure of the return annotation is not a subtree of "
            "the structure of the function return.\n\nFunction return: "
            f"{structure_out}\n\nReturn annotation: {structure_return}"
        )
        raise ValueError(msg)

    nodes = tree_leaves(task.produces["return"])
    values = structure_return.flatten_up_to(out)
    for node, value in zip(nodes, values):
        node.save(value)


def wrap_task_in_thread(task: PTask, **kwargs: Any) -> WrapperResult:
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
        _handle_task_function_return(task, out)
        exc_info = None
    return WrapperResult(
        python_nodes=None, warning_reports=[], exc_info=exc_info, stdout="", stderr=""
    )


def wrap_task_in_process(  # noqa: PLR0913
    task: PTask,
    kwargs: dict[str, Any],
    show_locals: bool,  # noqa: FBT001
    console_options: ConsoleOptions,
    session_filterwarnings: tuple[str, ...],
    task_filterwarnings: tuple[Mark, ...],
) -> WrapperResult:
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute in a
    spawned process or thread.

    """
    # Hide this function from tracebacks.
    __tracebackhide__ = True

    # Patch set_trace and breakpoint to show a better error message.
    _patch_set_trace_and_breakpoint()

    captured_stdout_buffer = StringIO()
    captured_stderr_buffer = StringIO()

    # Catch warnings and store them in a list.
    with warnings.catch_warnings(record=True) as log, redirect_stdout(
        captured_stdout_buffer
    ), redirect_stderr(captured_stderr_buffer):
        # Apply global filterwarnings.
        for arg in session_filterwarnings:
            warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        # Apply filters from "filterwarnings" marks
        for mark in task_filterwarnings:
            for arg in mark.args:
                warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        try:
            out = task.execute(**kwargs)
        except Exception:  # noqa: BLE001
            exc_info = sys.exc_info()
            processed_exc_info = _render_traceback_to_string(
                exc_info, show_locals, console_options
            )
        else:
            # Save products.
            _handle_task_function_return(task, out)
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

    captured_stdout_buffer.seek(0)
    captured_stderr_buffer.seek(0)
    captured_stdout = captured_stdout_buffer.read()
    captured_stderr = captured_stderr_buffer.read()
    captured_stdout_buffer.close()
    captured_stderr_buffer.close()

    # Collect all PythonNodes that are products to pass values back to the main process.
    python_nodes = tree_map(
        lambda x: x if isinstance(x, PythonNode) else None, task.produces
    )

    return WrapperResult(
        python_nodes=python_nodes,
        warning_reports=warning_reports,
        exc_info=processed_exc_info,
        stdout=captured_stdout,
        stderr=captured_stderr,
    )


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


def _render_traceback_to_string(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None],
    show_locals: bool,  # noqa: FBT001
    console_options: ConsoleOptions,
) -> tuple[type[BaseException], BaseException, str]:
    """Process the exception and convert the traceback to a string."""
    traceback = Traceback(exc_info, show_locals=show_locals)
    segments = console.render(traceback, options=console_options)
    text = "".join(segment.text for segment in segments)
    return (*exc_info[:2], text)
