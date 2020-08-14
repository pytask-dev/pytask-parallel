import os

from _pytask.config import hookimpl
from _pytask.shared import get_first_not_none_value
from pytask_parallel.callbacks import n_workers_callback


@hookimpl
def pytask_parse_config(config, config_from_cli, config_from_file):
    config["n_workers"] = get_first_not_none_value(
        config_from_cli,
        config_from_file,
        key="n_workers",
        default=1,
        callback=n_workers_callback,
    )
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)

    config["delay"] = get_first_not_none_value(
        config_from_cli, config_from_file, key="delay", default=0.1, callback=float
    )

    config["parallel_backend"] = get_first_not_none_value(
        config_from_cli,
        config_from_file,
        key="parallel_backend",
        default="processes",
        callback=_parallel_backend_callback,
    )


@hookimpl
def pytask_post_parse(config):
    """Disable parallelization if debugging is enabled."""
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1


def _parallel_backend_callback(x):
    if x not in ["processes", "threads"]:
        raise ValueError("parallel_backend has to be 'processes' or 'threads'.")
    return x
