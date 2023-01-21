"""Configure pytask."""
from __future__ import annotations

import enum
import os
from typing import Any

from pytask import hookimpl
from pytask_parallel.backends import ParallelBackendChoices


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    if config["n_workers"] == "auto":  # noqa: PLR2004
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    if (
        isinstance(config["parallel_backend"], str)
        and config["parallel_backend"] in ParallelBackendChoices._value2member_map_
    ):
        config["parallel_backend"] = ParallelBackendChoices(config["parallel_backend"])
    elif (
        isinstance(config["parallel_backend"], enum.Enum)
        and config["parallel_backend"] in ParallelBackendChoices
    ):
        pass
    else:
        raise ValueError("Invalid value for 'parallel_backend'.")

    config["delay"] = 0.1


@hookimpl
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1
