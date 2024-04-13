import coiled
from pytask import task


@task
@coiled.function(
    region="eu-central-1",  # Run the task close to you.
    memory="512 GB",  # Use a lot of memory.
    cpu=128,  # Use a lot of CPU.
    vm_type="p3.2xlarge",  # Run a GPU instance.
)
def task_example() -> None: ...
