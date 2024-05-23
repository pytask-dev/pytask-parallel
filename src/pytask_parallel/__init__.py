"""Contains the main namespace of the package."""

from __future__ import annotations

from pytask_parallel.backends import ParallelBackend
from pytask_parallel.backends import ParallelBackendRegistry
from pytask_parallel.backends import WorkerType
from pytask_parallel.backends import registry

try:
    from ._version import version as __version__
except ImportError:
    # broken installation, we don't even try unknown only works because we do poor mans
    # version compare
    __version__ = "unknown"


__all__ = [
    "ParallelBackend",
    "ParallelBackendRegistry",
    "WorkerType",
    "__version__",
    "registry",
]
