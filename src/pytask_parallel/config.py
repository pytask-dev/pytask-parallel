import os

from _pytask.config import hookimpl
from _pytask.shared import get_first_not_none_value


@hookimpl
def pytask_parse_config(config, config_from_cli, config_from_file):
    config["n_workers"] = get_first_not_none_value(
        config_from_cli, config_from_file, key="n_workers", default=1
    )
    if config["n_workers"] == "auto":
        config["n_workers"] = max(os.cpu_count() - 1, 1)
    else:
        config["n_workers"] = int(config["n_workers"])
    config["delay"] = get_first_not_none_value(
        config_from_cli, config_from_file, key="delay", default=0.1, callback=float
    )

    config["parallel_backend"] = get_first_not_none_value(
        config_from_cli, config_from_file, key="parallel_backend", default="processes"
    )
    if config["parallel_backend"] not in ["processes", "threads"]:
        raise ValueError("parallel_backend has to be 'processes' or 'threads'.")


@hookimpl
def pytask_post_parse(config):
    # Disable parallelization if debugging is enabled.
    if config["pdb"] or config["trace"]:
        config["n_workers"] = 1
