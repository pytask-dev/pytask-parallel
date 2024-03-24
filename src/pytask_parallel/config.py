"""Configure pytask."""

from __future__ import annotations

import enum
import os
from typing import Any

from pytask import hookimpl

from pytask_parallel.backends import ParallelBackend


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    if (
        isinstance(config["parallel_backend"], str)
        and config["parallel_backend"] in ParallelBackend._value2member_map_  # noqa: SLF001
    ):
        config["parallel_backend"] = ParallelBackend(config["parallel_backend"])
    elif (
        isinstance(config["parallel_backend"], enum.Enum)
        and config["parallel_backend"] in ParallelBackend
    ):
        pass
    else:
        msg = f"Invalid value for 'parallel_backend'. Got {config['parallel_backend']}."
        raise ValueError(msg)

    config["delay"] = 0.1


@hookimpl
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"] or config["dry_run"]:
        config["n_workers"] = 1
