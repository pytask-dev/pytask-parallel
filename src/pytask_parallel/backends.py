"""This module configures the available backends."""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum


try:
    from loky import get_reusable_executor

except ImportError:

    class ParallelBackendChoices(str, Enum):
        PROCESSES = "processes"
        THREADS = "threads"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackendChoices.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackendChoices.PROCESSES: ProcessPoolExecutor,
        ParallelBackendChoices.THREADS: ThreadPoolExecutor,
    }

else:

    class ParallelBackendChoices(str, Enum):  # type: ignore[no-redef]
        PROCESSES = "processes"
        THREADS = "threads"
        LOKY = "loky"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackendChoices.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackendChoices.PROCESSES: ProcessPoolExecutor,
        ParallelBackendChoices.THREADS: ThreadPoolExecutor,
        ParallelBackendChoices.LOKY: (  # type: ignore[attr-defined]
            get_reusable_executor
        ),
    }

    PARALLEL_BACKENDS_DEFAULT = (
        ParallelBackendChoices.LOKY  # type: ignore[attr-defined]
    )
