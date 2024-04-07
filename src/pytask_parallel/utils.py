"""Contains utility functions."""

from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from pytask.tree_util import PyTree
from pytask.tree_util import tree_map


if TYPE_CHECKING:
    from pytask_parallel.wrappers import WrapperResult
    from concurrent.futures import Future
    from pathlib import Path
    from types import ModuleType
    from types import TracebackType

    from pytask import PTask


__all__ = ["create_kwargs_for_task", "get_module", "parse_future_result"]


def parse_future_result(
    future: Future[WrapperResult],
) -> WrapperResult:
    """Parse the result of a future."""
    # An exception was raised before the task was executed.
    future_exception = future.exception()
    if future_exception is not None:
        from pytask_parallel.wrappers import WrapperResult

        exc_info = _parse_future_exception(future_exception)
        return WrapperResult(
            python_nodes=None,
            warning_reports=[],
            exc_info=exc_info,
            stdout="",
            stderr="",
        )
    return future.result()


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
