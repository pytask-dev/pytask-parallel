# pytask-parallel

[![PyPI](https://img.shields.io/pypi/v/pytask-parallel?color=blue)](https://pypi.org/project/pytask-parallel)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pytask-parallel)](https://pypi.org/project/pytask-parallel)
[![image](https://img.shields.io/conda/vn/conda-forge/pytask-parallel.svg)](https://anaconda.org/conda-forge/pytask-parallel)
[![image](https://img.shields.io/conda/pn/conda-forge/pytask-parallel.svg)](https://anaconda.org/conda-forge/pytask-parallel)
[![PyPI - License](https://img.shields.io/pypi/l/pytask-parallel)](https://pypi.org/project/pytask-parallel)
[![image](https://img.shields.io/github/actions/workflow/status/pytask-dev/pytask-parallel/main.yml?branch=main)](https://github.com/pytask-dev/pytask-parallel/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/pytask-dev/pytask-parallel/branch/main/graph/badge.svg)](https://codecov.io/gh/pytask-dev/pytask-parallel)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/pytask-dev/pytask-parallel/main.svg)](https://results.pre-commit.ci/latest/github/pytask-dev/pytask-parallel/main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

______________________________________________________________________

Parallelize the execution of tasks with `pytask-parallel`, a plugin for
[pytask](https://github.com/pytask-dev/pytask).

## Installation

pytask-parallel is available on [PyPI](https://pypi.org/project/pytask-parallel) and
[Anaconda.org](https://anaconda.org/conda-forge/pytask-parallel). Install it with

```console
$ pip install pytask-parallel

# or

$ conda install -c conda-forge pytask-parallel
```

By default, the plugin uses loky's reusable executor.

The following backends are available:

- loky's [`get_reusable_executor`](https://loky.readthedocs.io/en/stable/API.html#loky.get_reusable_executor)
- `ProcessPoolExecutor` or `ThreadPoolExecutor` from
  [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- dask's [`ClientExecutor`](https://distributed.dask.org/en/stable/api.html#distributed.Client.get_executor) allows in combination with [coiled](https://docs.coiled.io/user_guide/index.html) to spawn clusters and workers on AWS, GCP, and other providers with minimal configuration.

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
parallel_backend = "loky"  # or processes or threads
```

## Parallelization and Debugging

It is not possible to combine parallelization with debugging. That is why `--pdb` or
`--trace` deactivate parallelization.

If you parallelize the execution of your tasks using two or more workers, do not use
`breakpoint()` or `import pdb; pdb.set_trace()` since both will cause exceptions.

## Documentation

You find the documentation at <https://pytask-parallel.readthedocs.io/en/stable>.

## Changes

Consult the [release notes](https://pytask-parallel.readthedocs.io/en/stable/changes.html) to
find out about what is new.
