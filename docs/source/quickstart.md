# Quickstart

## Installation

pytask-parallel is available on [PyPI](https://pypi.org/project/pytask-parallel) and
[Anaconda.org](https://anaconda.org/conda-forge/pytask-parallel). Install it with

```console
$ pip install pytask-parallel

# or

$ conda install -c conda-forge pytask-parallel
```

## Usage

When the plugin is only installed and pytask executed, the tasks are not run in
parallel.

For parallelization with the default backend [loky](https://loky.readthedocs.io/), you need to launch multiple workers.

`````{tab-set}
````{tab-item} CLI
:sync: cli

```console
pytask -n 2
pytask --n-workers 2

# Starts os.cpu_count() - 1 workers.
pytask -n auto
```
````
````{tab-item} Configuration
:sync: configuration

```toml
[tool.pytask.ini_options]
n_workers = 2

# Starts os.cpu_count() - 1 workers.
n_workers = "auto"
```
````
`````

To use a different backend, pass the `--parallel-backend` option. The following command
will execute the workflow with one worker and the loky backend.

`````{tab-set}
````{tab-item} CLI
:sync: cli

```console
pytask --parallel-backend loky
```
````
````{tab-item} Configuration
:sync: configuration

```toml
[tool.pytask.ini_options]
parallel_backend = "loky"
```
````
`````

## Backends

```{important}
It is not possible to combine parallelization with debugging. That is why `--pdb` or
`--trace` deactivate parallelization.

If you parallelize the execution of your tasks using two or more workers, do not use
`breakpoint()` or `import pdb; pdb.set_trace()` since both will cause exceptions.
```

### loky

There are multiple backends available. The default is the backend provided by loky which
aims to be a more robust implementation of {class}`~multiprocessing.pool.Pool` and in
{class}`~concurrent.futures.ProcessPoolExecutor`.

```console
pytask --parallel-backend loky
```

As it spawns workers in new processes to run the tasks, it is especially suited for
CPU-bound tasks. ([Here](https://stackoverflow.com/a/868577/7523785) is an
explanation of what CPU- or IO-bound means.)

### `concurrent.futures`

You can use the values `threads` and `processes` to use the
{class}`~concurrent.futures.ThreadPoolExecutor` or the
{class}`~concurrent.futures.ProcessPoolExecutor` respectively.

The {class}`~concurrent.futures.ThreadPoolExecutor` might be an interesting option for
you if you have many IO-bound tasks and you do not need to create many expensive
processes.

`````{tab-set}
````{tab-item} CLI
:sync: cli

```console
pytask --parallel-backend threads
```
````
````{tab-item} Configuration
:sync: configuration

```toml
[tool.pytask.ini_options]
parallel_backend = "threads"
```
````
`````

`````{tab-set}
````{tab-item} CLI
:sync: cli

```console
pytask --parallel-backend processes
```
````
````{tab-item} Configuration
:sync: configuration

```toml
[tool.pytask.ini_options]
parallel_backend = "processes"
```
````
`````

```{important}
Capturing warnings is not thread-safe. Therefore, warnings cannot be captured reliably
when tasks are parallelized with `--parallel-backend threads`.
```

### dask + coiled

dask and coiled together provide the option to execute your workflow on cloud providers
like AWS, GCP or Azure. Check out the [dedicated guide](dask.md) if you are interested
in that.

Using the default mode, dask will spawn multiple local workers to process the tasks.

`````{tab-set}
````{tab-item} CLI
:sync: cli

```console
pytask --parallel-backend dask
```
````
````{tab-item} Configuration
:sync: configuration

```toml
[tool.pytask.ini_options]
parallel_backend = "dask"
```
````
`````

### Custom executors

You can also use any custom executor that implements the
{class}`~concurrent.futures.Executor` interface. Read more about it in
[](custom_executors.md).

```{important}
Please, consider contributing your executor to pytask-parallel if you believe it could
be helpful to other people. Start by creating an issue or a draft PR.
```
