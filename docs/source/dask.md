# Dask

```{caution}
Currently, the dask backend can only be used if your workflow code is organized in a
package due to how pytask imports your code and dask serializes task functions
([issue](https://github.com/dask/distributed/issues/8607)).
```

Dask is a flexible library for parallel and distributed computing. You probably know it
from its {class}`dask.dataframe` that allows lazy processing of big data. Here, we use
distributed that provides an interface similar to {class}`concurrent.futures.Executor`
to parallelize our execution.

There are a couple of ways in which we can use dask.

## Local

Using dask as the parallel backend will launch a {class}`distributed.LocalCluster` with
processes on your local machine.

`````{tab-set}
````{tab-item} CLI
```console
pytask --parallel-backend dask -n 2
```
````
````{tab-item} Configuration
```toml
[tool.pytask.ini_options]
parallel_backend = "dask"
n_workers = 2
```
````
`````

## Local or Remote - Connecting to a Scheduler

It is also possible to connect to an existing scheduler and use it to execute tasks. The
scheduler can be launched on your local machine or in some remote environment. It also
has the benefit of being able to inspect the dask dashboard for more information on the
execution.

Start by launching a scheduler in some terminal on some machine.

```console
dask scheduler
```

After the launch, the IP of the scheduler will be displayed. Copy it. Then, open more
terminals to launch as many dask workers as you like with

```console
dask worker <scheduler-ip>
```

Finally, write a function to build the dask client and register it as the backend.

```{important}
Place the following code in any module that will be imported when you are executing
pytask. For example, the `src/project/config.py` in your project, the
`src/project/__init__.py` or the task module directly.
```

```python
from pytask_parallel import ParallelBackend
from pytask_parallel import registry
from concurrent.futures import Executor
from dask.distributed import Client


def _build_dask_executor(n_workers: int) -> Executor:
    return Client(address="<scheduler-ip>").get_executor()


registry.register_parallel_backend(ParallelBackend.DASK, _build_dask_executor)
```

You can also register it as the custom executor using
{obj}`pytask_parallel.ParallelBackend.CUSTOM` to switch back to the default dask
executor quickly.

```{seealso}
You can find more information in the documentation for
[`dask.distributed`](https://distributed.dask.org/en/stable/).
```

## Remote

You can learn how to deploy your tasks to a remote dask cluster in
[this guide](https://docs.dask.org/en/stable/deploying.html). They recommend using
coiled for deployment to cloud providers.

[coiled](https://www.coiled.io/) is a product built on top of dask that eases the
deployment of your workflow to many cloud providers like AWS, GCP, and Azure.

If you want to run the tasks in your project on a cluster managed by coiled read
{ref}`this guide <coiled-clusters>`.

Otherwise, follow the instructions in
[dask's guide](https://docs.dask.org/en/stable/deploying.html).
