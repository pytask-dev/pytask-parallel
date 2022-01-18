"""Entry-point for the plugin."""
from _pytask.config import hookimpl
from pluggy import PluginManager
from pytask_parallel import build
from pytask_parallel import config
from pytask_parallel import execute
from pytask_parallel import logging


@hookimpl
def pytask_add_hooks(pm: PluginManager) -> None:
    """Register plugins."""
    pm.register(build)
    pm.register(config)
    pm.register(execute)
    pm.register(logging)
