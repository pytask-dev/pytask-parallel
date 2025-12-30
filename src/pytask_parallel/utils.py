"""Contains utility functions."""

from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING
from typing import Any

from pytask import NodeLoadError
from pytask import PNode
from pytask import PPathNode
from pytask import PProvisionalNode
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map_with_path

from pytask_parallel.nodes import RemotePathNode
from pytask_parallel.typing import is_local_path

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future
    from pathlib import Path
    from types import ModuleType
    from types import TracebackType

    from pytask import PTask

    from pytask_parallel.wrappers import WrapperResult

try:
    from coiled.function import Function as CoiledFunction
except ImportError:

    class CoiledFunction: ...


__all__ = [
    "create_kwargs_for_task",
    "get_module",
    "parse_future_result",
]


def parse_future_result(
    future: Future[WrapperResult],
) -> WrapperResult:
    """Parse the result of a future."""
    # An exception was raised before the task was executed.
    future_exception = future.exception()
    if future_exception is not None:
        # Prevent circular import for loky backend.
        from pytask_parallel.wrappers import WrapperResult  # noqa: PLC0415

        exc_info = _parse_future_exception(future_exception)
        return WrapperResult(
            carry_over_products=None,  # type: ignore[arg-type]
            warning_reports=[],
            exc_info=exc_info,
            stdout="",
            stderr="",
        )
    return future.result()


def _safe_load(
    path: tuple[Any, ...],
    node: PNode | PProvisionalNode,
    task: PTask,
    *,
    is_product: bool,
    remote: bool,
) -> Any:
    """Load a node and catch exceptions."""
    _rich_traceback_guard = True
    # Get the argument name like "path" or "return" for function returns.
    argument = path[0]

    # Replace local path nodes with remote path nodes if necessary.
    if (
        remote
        and argument != "return"
        and isinstance(node, PPathNode)
        and is_local_path(node.path)
    ):
        return RemotePathNode.from_path_node(node, is_product=is_product)

    try:
        return node.load(is_product=is_product)
    except Exception as e:
        msg = f"Exception while loading node {node.name!r} of task {task.name!r}"
        raise NodeLoadError(msg) from e


def create_kwargs_for_task(task: PTask, *, remote: bool) -> dict[str, PyTree[Any]]:
    """Create kwargs for task function."""
    parameters = inspect.signature(task.function).parameters

    kwargs = {}

    for name, value in task.depends_on.items():
        kwargs[name] = tree_map_with_path(
            lambda p, x: _safe_load(
                (name, *p),  # noqa: B023
                x,
                task,
                is_product=False,
                remote=remote,
            ),
            value,
        )

    for name, value in task.produces.items():
        if name in parameters:
            kwargs[name] = tree_map_with_path(
                lambda p, x: _safe_load(
                    (name, *p),  # noqa: B023
                    x,
                    task,
                    is_product=True,
                    remote=remote,
                ),
                value,
            )

    return kwargs


def _parse_future_exception(
    exc: BaseException | None,
) -> tuple[type[BaseException], BaseException, TracebackType | None] | None:
    """Parse a future exception into the format of ``sys.exc_info``."""
    return None if exc is None else (type(exc), exc, exc.__traceback__)


def get_module(func: Callable[..., Any], path: Path | None) -> ModuleType:
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
        return inspect.getmodule(func, path.as_posix())  # type: ignore[return-value]
    return inspect.getmodule(func)  # type: ignore[return-value]
