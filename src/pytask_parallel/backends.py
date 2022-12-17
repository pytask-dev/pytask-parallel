"""This module configures the available backends."""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum


try:
    from loky import get_reusable_executor

except ImportError:

    class ParallelBackendsChoices(str, Enum):
        PROCESSES = "processes"
        THREADS = "threads"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackendsChoices.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackendsChoices.PROCESSES: ProcessPoolExecutor,
        ParallelBackendsChoices.THREADS: ThreadPoolExecutor,
    }

else:

    class ParallelBackendsChoices(str, Enum):  # type: ignore[no-redef]
        PROCESSES = "processes"
        THREADS = "threads"
        LOKY = "loky"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackendsChoices.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackendsChoices.PROCESSES: ProcessPoolExecutor,
        ParallelBackendsChoices.THREADS: ThreadPoolExecutor,
        ParallelBackendsChoices.LOKY: (  # type: ignore[attr-defined]
            get_reusable_executor
        ),
    }

    PARALLEL_BACKENDS_DEFAULT = (
        ParallelBackendsChoices.LOKY  # type: ignore[attr-defined]
    )
