"""Configure pytask."""

from __future__ import annotations

import os
from typing import Any

from pytask import hookimpl

from pytask_parallel import execute
from pytask_parallel import logging
from pytask_parallel.backends import ParallelBackend


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    __tracebackhide__ = True

    try:
        config["parallel_backend"] = ParallelBackend(config["parallel_backend"])
    except ValueError:
        msg = (
            f"Invalid value for 'parallel_backend'. Got {config['parallel_backend']}. "
            f"Choose one of {', '.join([e.value for e in ParallelBackend])}."
        )
        raise ValueError(msg) from None

    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    # If more than one worker is used, and no backend is set, use loky.
    if config["n_workers"] > 1 and config["parallel_backend"] == ParallelBackend.NONE:
        config["parallel_backend"] = ParallelBackend.LOKY

    config["delay"] = 0.1


@hookimpl(trylast=True)
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Register the parallel backend if debugging is not enabled."""
    # Deactivate parallel execution if debugger, trace or dry-run is used.
    if config["pdb"] or config["trace"] or config["dry_run"]:
        return

    if config["parallel_backend"] == ParallelBackend.NONE:
        return

    # Register parallel execute and logging hook.
    config["pm"].register(execute)
    config["pm"].register(logging)
