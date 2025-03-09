"""Contains functions related to typing."""  # noqa: A005

from pathlib import Path
from pathlib import PosixPath
from pathlib import WindowsPath
from typing import NamedTuple

from pytask import PTask
from upath.implementations.local import FilePath

__all__ = ["is_coiled_function", "is_local_path"]


def is_coiled_function(task: PTask) -> bool:
    """Check if a function is a coiled function."""
    return "coiled_kwargs" in task.attributes


def is_local_path(path: Path) -> bool:
    """Check if a path is local."""
    return isinstance(path, (FilePath, PosixPath, WindowsPath))


class CarryOverPath(NamedTuple):
    content: bytes
