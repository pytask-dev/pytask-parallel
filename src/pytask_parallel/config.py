"""Configure pytask."""
from __future__ import annotations

import os
from typing import Any
from typing import Callable

from pytask import hookimpl
from pytask_parallel.backends import PARALLEL_BACKENDS_DEFAULT
from pytask_parallel.callbacks import delay_callback
from pytask_parallel.callbacks import n_workers_callback
from pytask_parallel.callbacks import parallel_backend_callback


@hookimpl
def pytask_parse_config(config, config_from_cli, config_from_file):
    """Parse the configuration."""
    config["n_workers"] = _get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="n_workers",
        default=1,
        callback=n_workers_callback,
    )
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    config["delay"] = _get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="delay",
        default=0.1,
        callback=delay_callback,
    )

    config["parallel_backend"] = _get_first_non_none_value(
        config_from_cli,
        config_from_file,
        key="parallel_backend",
        default=PARALLEL_BACKENDS_DEFAULT,
        callback=parallel_backend_callback,
    )


@hookimpl
def pytask_post_parse(config):
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1


def _get_first_non_none_value(
    *configs: dict[str, Any],
    key: str,
    default: Any | None = None,
    callback: Callable[..., Any] | None = None,
) -> Any:
    """Get the first non-None value for a key from a list of dictionaries.

    This function allows to prioritize information from many configurations by changing
    the order of the inputs while also providing a default.

    """
    callback = (lambda x: x) if callback is None else callback  # noqa: E731
    processed_values = (callback(config.get(key)) for config in configs)
    return next((value for value in processed_values if value is not None), default)
