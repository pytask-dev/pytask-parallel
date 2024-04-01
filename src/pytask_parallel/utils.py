"""Contains utility functions."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING
from typing import Any

from pytask.tree_util import PyTree
from pytask.tree_util import tree_leaves
from pytask.tree_util import tree_map
from pytask.tree_util import tree_structure

if TYPE_CHECKING:
    from concurrent.futures import Future
    from types import TracebackType

    from pytask import PTask
    from pytask import PythonNode
    from pytask import WarningReport


def parse_future_result(
    future: Future[Any],
) -> tuple[
    dict[str, PyTree[PythonNode | None]] | None,
    list[WarningReport],
    tuple[type[BaseException], BaseException, TracebackType] | None,
]:
    """Parse the result of a future."""
    # An exception was raised before the task was executed.
    future_exception = future.exception()
    if future_exception is not None:
        exc_info = _parse_future_exception(future_exception)
        return None, [], exc_info

    out = future.result()
    if isinstance(out, tuple) and len(out) == 3:  # noqa: PLR2004
        return out

    if out is None:
        return None, [], None

    # What to do when the output does not match?
    msg = (
        "The task function returns an unknown output format. Either return a tuple "
        "with three elements, python nodes, warning reports and exception or only "
        "return."
    )
    raise Exception(msg)  # noqa: TRY002


def handle_task_function_return(task: PTask, out: Any) -> None:
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


def create_kwargs_for_task(task: PTask) -> dict[str, PyTree[Any]]:
    """Create kwargs for task function."""
    parameters = inspect.signature(task.function).parameters

    kwargs = {}
    for name, value in task.depends_on.items():
        kwargs[name] = tree_map(lambda x: x.load(), value)

    for name, value in task.produces.items():
        if name in parameters:
            kwargs[name] = tree_map(lambda x: x.load(), value)

    return kwargs


def _parse_future_exception(
    exc: BaseException | None,
) -> tuple[type[BaseException], BaseException, TracebackType] | None:
    """Parse a future exception into the format of ``sys.exc_info``."""
    return None if exc is None else (type(exc), exc, exc.__traceback__)
