"""Configure pytask."""
from __future__ import annotations

import os
from typing import Any

from pytask import hookimpl


@hookimpl
def pytask_parse_config(config: dict[str, Any]) -> None:
    """Parse the configuration."""
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    config["delay"] = 0.1


@hookimpl
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1
