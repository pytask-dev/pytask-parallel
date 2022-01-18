"""Configure pytask."""
import os
from typing import Any
from typing import Dict

from _pytask.config import hookimpl
from _pytask.shared import get_first_non_none_value
from pytask_parallel.backends import PARALLEL_BACKENDS_DEFAULT
from pytask_parallel.callbacks import delay_callback
from pytask_parallel.callbacks import n_workers_callback
from pytask_parallel.callbacks import parallel_backend_callback


@hookimpl
def pytask_parse_config(
    config: Dict[str, Any],
    config_from_cli: Dict[str, Any],
    config_from_file: Dict[str, Any],
) -> None:
    """Parse the configuration."""
    config["n_workers"] = get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="n_workers",
        default=1,
        callback=n_workers_callback,
    )
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    config["delay"] = get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="delay",
        default=0.1,
        callback=delay_callback,
    )

    config["parallel_backend"] = get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="parallel_backend",
        default=PARALLEL_BACKENDS_DEFAULT,
        callback=parallel_backend_callback,
    )


@hookimpl
def pytask_post_parse(config: Dict[str, Any]) -> None:
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1
