from _pytask.config import hookimpl
from pytask_parallel import cli
from pytask_parallel import config
from pytask_parallel import execute


@hookimpl
def pytask_add_hooks(pm):
    pm.register(cli)
    pm.register(config)
    pm.register(execute)
