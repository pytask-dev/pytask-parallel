# Dask

```{caution}
Currently, the dask backend can only be used if your workflow code is organized in a
package due to how pytask imports your code and dask serializes task functions
([issue](https://github.com/dask/distributed/issues/8607)).
```

Dask is a flexible library for parallel and distributed computing. You probably know it
from its {class}`dask.dataframe` that allows lazy processing of big data. Here, we use
{mod}`distributed` that provides an interface similar to
{class}`~concurrent.futures.Executor` to parallelize our execution.

There are a couple of ways in how we can use dask.

## Local

By default, using dask as the parallel backend will launch a
{class}`distributed.LocalCluster` with processes on your local machine.

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

Finally, write a function to build the dask client and register it as the dask backend.
Place the code somewhere in your codebase, preferably, where you store the main
configuration of your project in `config.py` or another module that will be imported
during execution.

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
{class}`pytask_parallel.ParallelBackend.CUSTOM` to switch back to the default dask
executor quickly.

```{seealso}
You can find more information in the documentation for
[`dask.distributed`](https://distributed.dask.org/en/stable/).
```

## Remote - Using cloud providers with coiled

[coiled](https://www.coiled.io/) is a product built on top of dask that eases the
deployment of your workflow to many cloud providers like AWS, GCP, and Azure.

They offer a [free monthly tier](https://www.coiled.io/pricing) where you only
need to pay the costs for your cloud provider and you can get started without a credit
card.

Furthermore, they offer the following benefits which are especially helpful to people
who are not familiar with cloud providers or remote computing.

- A [four step short process](https://docs.coiled.io/user_guide/setup/index.html) to set
  up your local environment and configure your cloud provider.
- coiled manages your resources by spawning workers if you need them and shutting them
  down if they are idle.
- Synchronization of your local environment to remote workers.

So, how can you run your pytask workflow on a cloud infrastructure with coiled?

1. Follow their [guide on getting
   started](https://docs.coiled.io/user_guide/setup/index.html) by creating a coiled
   account and syncing it with your cloud provider.

1. Register a function that builds an executor using {class}`coiled.Cluster`.

   ```python
   import coiled
   from pytask_parallel import ParallelBackend
   from pytask_parallel import registry
   from concurrent.futures import Executor


   def _build_coiled_executor(n_workers: int) -> Executor:
       return coiled.Cluster(n_workers=n_workers).get_client().get_executor()


   registry.register_parallel_backend(ParallelBackend.CUSTOM, _build_coiled_executor)
   ```

1. Execute your workflow with

   ```console
   pytask --parallel-backend custom
   ```
