"""Entry-point for the plugin."""
from __future__ import annotations

from pluggy import PluginManager
from pytask import hookimpl
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
