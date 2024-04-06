"""Configure pytask."""

from __future__ import annotations

import os
from typing import Any

from pytask import hookimpl

from pytask_parallel import custom
from pytask_parallel import dask
from pytask_parallel import execute
from pytask_parallel import logging
from pytask_parallel import processes
from pytask_parallel import threads
from pytask_parallel.backends import ParallelBackend


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    __tracebackhide__ = True

    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    try:
        config["parallel_backend"] = ParallelBackend(config["parallel_backend"])
    except ValueError:
        msg = (
            f"Invalid value for 'parallel_backend'. Got {config['parallel_backend']}. "
            f"Choose one of {', '.join([e.value for e in ParallelBackend])}."
        )
        raise ValueError(msg) from None

    config["delay"] = 0.1


@hookimpl(trylast=True)
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Register the parallel backend if debugging is not enabled."""
    # Deactivate parallel execution if debugger, trace or dry-run is used.
    if config["pdb"] or config["trace"] or config["dry_run"]:
        return

    # Register parallel execute and logging hook.
    config["pm"].register(logging)
    config["pm"].register(execute)

    # Register parallel backends.
    if config["parallel_backend"] == ParallelBackend.THREADS:
        config["pm"].register(threads)
    elif config["parallel_backend"] == ParallelBackend.DASK:
        config["pm"].register(dask)
    elif config["parallel_backend"] == ParallelBackend.CUSTOM:
        config["pm"].register(custom)
    else:
        config["pm"].register(processes)
