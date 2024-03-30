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
    from pytask import PTask


def _handle_task_function_return(task: PTask, out: Any) -> None:
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


def _create_kwargs_for_task(task: PTask) -> dict[str, PyTree[Any]]:
    """Create kwargs for task function."""
    parameters = inspect.signature(task.function).parameters

    kwargs = {}
    for name, value in task.depends_on.items():
        kwargs[name] = tree_map(lambda x: x.load(), value)

    for name, value in task.produces.items():
        if name in parameters:
            kwargs[name] = tree_map(lambda x: x.load(), value)

    return kwargs
