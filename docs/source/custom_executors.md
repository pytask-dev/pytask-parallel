# Custom Executors

> [!NOTE]
>
> The interface for custom executors is rudimentary right now and there is not a lot of
> support by public functions. Please, give some feedback if you are trying or managed
> to use a custom backend.
>
> Also, please contribute your custom executors if you consider them useful to others.

pytask-parallel allows you to use your parallel backend as long as it follows the
interface defined by
[`concurrent.futures.Executor`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor).

In some cases, adding a new backend can be as easy as registering a builder function
that receives some arguments (currently only `n_workers`) and returns the instantiated
executor.

```python
from concurrent.futures import Executor
from my_project.executor import CustomExecutor

from pytask_parallel import ParallelBackend, registry


def build_custom_executor(n_workers: int) -> Executor:
    return CustomExecutor(max_workers=n_workers)


registry.register_parallel_backend(ParallelBackend.CUSTOM, build_custom_executor)
```

Now, build the project requesting your custom backend.

```console
pytask --parallel-backend custom
```

Realistically, it is not the only necessary adjustment for a nice user experience. There
are two other important things. pytask-parallel does not implement them by default since
it seems more tightly coupled to your backend.

1. A wrapper for the executed function that captures warnings, catches exceptions and
   saves products of the task (within the child process!).

   As an example, see
   [`def _execute_task()`](https://github.com/pytask-dev/pytask-parallel/blob/c441dbb75fa6ab3ab17d8ad5061840c802dc1c41/src/pytask_parallel/processes.py#L91-L155)
   that does all that for the processes and loky backend.

1. To apply the wrapper, you need to write a custom hook implementation for
   `def pytask_execute_task()`. See
   [`def pytask_execute_task()`](https://github.com/pytask-dev/pytask-parallel/blob/c441dbb75fa6ab3ab17d8ad5061840c802dc1c41/src/pytask_parallel/processes.py#L41-L65)
   for an example. Use the
   [`hook_module`](https://pytask-dev.readthedocs.io/en/stable/how_to_guides/extending_pytask.html#using-hook-module-and-hook-module)
   configuration value to register your implementation.

Another example of an implementation can be found as a
[test](https://github.com/pytask-dev/pytask-parallel/blob/c441dbb75fa6ab3ab17d8ad5061840c802dc1c41/tests/test_backends.py#L35-L78).
