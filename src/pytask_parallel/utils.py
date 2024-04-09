"""Contains utility functions."""

from __future__ import annotations

import inspect
from functools import partial
from pathlib import PosixPath
from pathlib import WindowsPath
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from pytask import NodeLoadError
from pytask import PathNode
from pytask import PNode
from pytask import PProvisionalNode
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map_with_path
from upath.implementations.local import FilePath

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path
    from types import ModuleType
    from types import TracebackType

    from pytask import PTask

    from pytask_parallel.wrappers import WrapperResult


__all__ = [
    "create_kwargs_for_task",
    "get_module",
    "parse_future_result",
    "is_local_path",
]


def parse_future_result(
    future: Future[WrapperResult],
) -> WrapperResult:
    """Parse the result of a future."""
    # An exception was raised before the task was executed.
    future_exception = future.exception()
    if future_exception is not None:
        # Prevent circular import for loky backend.
        from pytask_parallel.wrappers import WrapperResult

        exc_info = _parse_future_exception(future_exception)
        return WrapperResult(
            carry_over_products=None,
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

    # Raise an error if a PPathNode with a local path is used as a dependency or product
    # (except as a return value).
    if (
        remote
        and argument != "return"
        and isinstance(node, PathNode)
        and is_local_path(node.path)
    ):
        if is_product:
            msg = (
                f"You cannot use a local path as a product in argument {argument!r} "
                "with a remote backend. Either return the content that should be saved "
                "in the file with a return annotation "
                "(https://tinyurl.com/pytask-return) or use a nonlocal path to store "
                "the file in S3 or their like https://tinyurl.com/pytask-remote."
            )
            raise NodeLoadError(msg)
        msg = (
            f"You cannot use a local path as a dependency in argument {argument!r} "
            "with a remote backend. Upload the file to a remote storage like S3 "
            "and use the remote path instead: https://tinyurl.com/pytask-remote."
        )
        raise NodeLoadError(msg)

    try:
        return node.load(is_product=is_product)
    except Exception as e:  # noqa: BLE001
        msg = f"Exception while loading node {node.name!r} of task {task.name!r}"
        raise NodeLoadError(msg) from e


def create_kwargs_for_task(task: PTask, *, remote: bool) -> dict[str, PyTree[Any]]:
    """Create kwargs for task function."""
    parameters = inspect.signature(task.function).parameters

    kwargs = {}

    for name, value in task.depends_on.items():
        kwargs[name] = tree_map_with_path(
            lambda p, x: _safe_load(
                (name, *p),
                x,
                task,
                is_product=False,
                remote=remote,  # noqa: B023
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
) -> tuple[type[BaseException], BaseException, TracebackType] | None:
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
        return inspect.getmodule(func, path.as_posix())
    return inspect.getmodule(func)


def is_local_path(path: Path) -> bool:
    """Check if a path is local."""
    return isinstance(path, (FilePath, PosixPath, WindowsPath))
