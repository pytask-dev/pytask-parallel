import coiled
from pytask import task


@task
@coiled.function()
def task_example() -> None: ...
