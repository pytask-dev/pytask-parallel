# pytask-parallel

[![PyPI](https://img.shields.io/pypi/v/pytask-parallel?color=blue)](https://pypi.org/project/pytask-parallel)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pytask-parallel)](https://pypi.org/project/pytask-parallel)
[![image](https://img.shields.io/conda/vn/conda-forge/pytask-parallel.svg)](https://anaconda.org/conda-forge/pytask-parallel)
[![image](https://img.shields.io/conda/pn/conda-forge/pytask-parallel.svg)](https://anaconda.org/conda-forge/pytask-parallel)
[![PyPI - License](https://img.shields.io/pypi/l/pytask-parallel)](https://pypi.org/project/pytask-parallel)
[![image](https://img.shields.io/github/actions/workflow/status/pytask-dev/pytask-parallel/main.yml?branch=main)](https://github.com/pytask-dev/pytask-parallel/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/pytask-dev/pytask-parallel/branch/main/graph/badge.svg)](https://codecov.io/gh/pytask-dev/pytask-parallel)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/pytask-dev/pytask-parallel/main.svg)](https://results.pre-commit.ci/latest/github/pytask-dev/pytask-parallel/main)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

______________________________________________________________________

Parallelize the execution of tasks with `pytask-parallel` which is a plugin for
[pytask](https://github.com/pytask-dev/pytask).

## Installation

pytask-parallel is available on [PyPI](https://pypi.org/project/pytask-parallel) and
[Anaconda.org](https://anaconda.org/conda-forge/pytask-parallel). Install it with

```console
$ pip install pytask-parallel

# or

$ conda install -c conda-forge pytask-parallel
```

By default, the plugin uses `concurrent.futures.ProcessPoolExecutor`.

It is also possible to select the executor from loky or `ThreadPoolExecutor` from the
[concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) module
as backends to execute tasks asynchronously.

## Usage

To parallelize your tasks across many workers, pass an integer greater than 1 or
`'auto'` to the command-line interface.

```console
$ pytask -n 2
$ pytask --n-workers 2

# Starts os.cpu_count() - 1 workers.
$ pytask -n auto
```

Using processes to parallelize the execution of tasks is useful for CPU-bound tasks such
as numerical computations. ([Here](https://stackoverflow.com/a/868577/7523785) is an
explanation of what CPU- or IO-bound means.)

For IO-bound tasks, tasks where the limiting factor is network latency and access to
files, you can parallelize via threads.

```console
pytask --parallel-backend threads
```

You can also set the options in a `pyproject.toml`.

```toml
# This is the default configuration. Note that, parallelization is turned off.

[tool.pytask.ini_options]
n_workers = 1
parallel_backend = "processes"  # or loky or threads
```

## Custom Executor

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

## Some implementation details

### Parallelization and Debugging

It is not possible to combine parallelization with debugging. That is why `--pdb` or
`--trace` deactivate parallelization.

If you parallelize the execution of your tasks using two or more workers, do not use
`breakpoint()` or `import pdb; pdb.set_trace()` since both will cause exceptions.

### Threads and warnings

Capturing warnings is not thread-safe. Therefore, warnings cannot be captured reliably
when tasks are parallelized with `--parallel-backend threads`.

## Changes

Consult the [release notes](CHANGES.md) to find out about what is new.

## Development

- `pytask-parallel` does not call the `pytask_execute_task_protocol` hook
  specification/entry-point because `pytask_execute_task_setup` and
  `pytask_execute_task` need to be separated from `pytask_execute_task_teardown`. Thus,
  plugins that change this hook specification may not interact well with the
  parallelization.

- Two PRs for CPython try to re-enable setting custom reducers which should have been
  working but does not. Here are the references.

  - https://bugs.python.org/issue28053
  - https://github.com/python/cpython/pull/9959
  - https://github.com/python/cpython/pull/15058
