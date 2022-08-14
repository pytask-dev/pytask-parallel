"""Validate command line inputs and configuration values."""
from __future__ import annotations

from typing import Any

from pytask_parallel.backends import PARALLEL_BACKENDS


def n_workers_callback(value: Any) -> int:
    """Validate the n-workers option."""
    if value == "auto":
        pass
    elif value in [None, "None", "none"]:
        value = None
    elif isinstance(value, int) and 1 <= value:
        pass
    elif isinstance(value, str) and value.isdigit():
        value = int(value)
    else:
        raise ValueError("n_workers can either be an integer >= 1, 'auto' or None.")

    return value


def parallel_backend_callback(value: Any) -> str | None:
    """Validate the input for the parallel backend."""
    if value in [None, "None", "none"]:
        value = None
    elif value in PARALLEL_BACKENDS:
        pass
    else:
        raise ValueError(
            f"parallel_backend has to be one of {list(PARALLEL_BACKENDS)}."
        )
    return value
