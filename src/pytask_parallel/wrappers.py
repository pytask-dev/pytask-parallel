"""Contains functions related to processes and loky."""

from __future__ import annotations

import functools
import sys
import warnings
from contextlib import redirect_stderr
from contextlib import redirect_stdout
from io import StringIO
from typing import TYPE_CHECKING
from typing import Any

from attrs import define
from pytask import PNode
from pytask import PPathNode
from pytask import PTask
from pytask import PythonNode
from pytask import Traceback
from pytask import WarningReport
from pytask import console
from pytask import parse_warning_filter
from pytask import warning_record_to_str
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map_with_path
from pytask.tree_util import tree_structure

from pytask_parallel.utils import CoiledFunction
from pytask_parallel.utils import is_local_path

if TYPE_CHECKING:
    from types import TracebackType

    from pytask import Mark
    from rich.console import ConsoleOptions


__all__ = ["wrap_task_in_process", "wrap_task_in_thread"]


@define(kw_only=True)
class WrapperResult:
    carry_over_products: PyTree[PythonNode | None]
    warning_reports: list[WarningReport]
    exc_info: tuple[type[BaseException], BaseException, TracebackType | str] | None
    stdout: str
    stderr: str


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
        _handle_function_products(task, out, remote=False)
        exc_info = None
    return WrapperResult(
        carry_over_products=None,
        warning_reports=[],
        exc_info=exc_info,
        stdout="",
        stderr="",
    )


def wrap_task_in_process(  # noqa: PLR0913
    task: PTask,
    *,
    console_options: ConsoleOptions,
    kwargs: dict[str, Any],
    remote: bool,
    session_filterwarnings: tuple[str, ...],
    show_locals: bool,
    task_filterwarnings: tuple[Mark, ...],
) -> WrapperResult:
    """Execute a task in a spawned process.

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
            products = None
        else:
            # Save products.
            products = _handle_function_products(task, out, remote=remote)
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

    return WrapperResult(
        carry_over_products=products,
        warning_reports=warning_reports,
        exc_info=processed_exc_info,
        stdout=captured_stdout,
        stderr=captured_stderr,
    )


def rewrap_task_with_coiled_function(task: PTask) -> CoiledFunction:
    return functools.wraps(wrap_task_in_process)(
        CoiledFunction(wrap_task_in_process, **task.attributes["coiled_kwargs"])
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


def _handle_function_products(
    task: PTask, out: Any, *, remote: bool
) -> PyTree[PythonNode | None]:
    """Handle the products of the task.

    The functions first responsibility is to push the returns of the function to the
    defined nodes.

    Secondly, the function collects two kinds of products that need to be carried over
    to the main process for storing them.

    1. Any product that is a :class:`~pytask.PythonNode` needs to be carried over to the
       main process as otherwise their value would be lost.
    2. If the function is executed remotely and the return value should be stored in a
       node with a local path like :class:`pytask.PickleNode`, we need to carry over the
       value to the main process again and, then, save the value to the node as the
       local path does not exist remotely.

    """
    # Check that the return value has the correct structure.
    if "return" in task.produces:
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

    def _save_and_carry_over_product(
        path: tuple[Any, ...], node: PNode
    ) -> PythonNode | None:
        argument = path[0]

        if argument != "return":
            if isinstance(node, PythonNode):
                return node
            return None

        value = out
        for p in path[1:]:
            value = value[p]

        # If the node is a PythonNode, we need to carry it over to the main process.
        if isinstance(node, PythonNode):
            node.save(value)
            return node

        # If the path is local and we are remote, we need to carry over the value to
        # the main process as a PythonNode and save it later.
        if remote and isinstance(node, PPathNode) and is_local_path(node.path):
            return PythonNode(value=value)

        # If no condition applies, we save the value and do not carry it over. Like a
        # remote path to S3.
        node.save(value)
        return None

    return tree_map_with_path(_save_and_carry_over_product, task.produces)
