"""Configure pytask."""

from __future__ import annotations

import os
from typing import Any

from pytask import hookimpl

from pytask_parallel.backends import ParallelBackend


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    try:
        config["parallel_backend"] = ParallelBackend(config["parallel_backend"])
    except ValueError:
        msg = f"Invalid value for 'parallel_backend'. Got {config['parallel_backend']}."
        raise ValueError(msg) from None

    config["delay"] = 0.1


@hookimpl(trylast=True)
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"] or config["dry_run"]:
        config["n_workers"] = 1

    if config["n_workers"] > 1:
        if config["parallel_backend"] == ParallelBackend.THREADS:
            from pytask_parallel import threads

            config["pm"].register(threads)

        elif config["parallel_backend"] in (
            ParallelBackend.LOKY,
            ParallelBackend.PROCESSES,
        ):
            from pytask_parallel import processes

            config["pm"].register(processes)

    if config["n_workers"] > 1 or config["parallel_backend"] == ParallelBackend.CUSTOM:
        from pytask_parallel import execute

        config["pm"].register(execute)
