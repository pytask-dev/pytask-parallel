"""This module configures the available backends."""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor


PARALLEL_BACKENDS = {
    "processes": ProcessPoolExecutor,
    "threads": ThreadPoolExecutor,
}

PARALLEL_BACKENDS_DEFAULT = "processes"

try:
    from loky import get_reusable_executor
except ImportError:
    pass
else:
    PARALLEL_BACKENDS["loky"] = get_reusable_executor
    PARALLEL_BACKENDS_DEFAULT = "loky"
