# coiled

```{caution}
Currently, the coiled backend can only be used if your workflow code is organized as a
package due to how pytask imports your code and dask serializes task functions
([issue](https://github.com/dask/distributed/issues/8607)).
```

[coiled](https://www.coiled.io/) is a product built on top of dask that eases the
deployment of your workflow to many cloud providers like AWS, GCP, and Azure.

Note that, coiled is a paid service. They offer a
[free monthly tier](https://www.coiled.io/pricing) where you only need to pay the costs
for your cloud provider and you can get started without a credit card.

They provide the following benefits which are especially helpful to people who are not
familiar with cloud providers or remote computing.

- coiled manages your resources by spawning workers if you need them and shutting them
  down if they are idle.
- [Synchronization](https://docs.coiled.io/user_guide/software/sync.html) of your local
  environment to remote workers.
- [Adaptive scaling](https://docs.dask.org/en/latest/adaptive.html) if your workflow
  takes a long time to finish.

There are two ways how you can use coiled with pytask and pytask-parallel.

1. Run individual tasks in the cloud.
1. Run your whole workflow in the cloud.

Both approaches are explained below after the setup.

## Setup

Follow coiled's
[four step short process](https://docs.coiled.io/user_guide/setup/index.html) to set up
your local environment and configure your cloud provider.

## Running individual tasks

In most projects there are a just couple of tasks that require a lot of resources and
that you would like to run in a virtual machine in the cloud.

With coiled's
[serverless functions](https://docs.coiled.io/user_guide/usage/functions/index.html),
you can define the hardware and software environment for your task. Just decorate the
task function with a {func}`@coiled.function <coiled.function>` decorator.

```{literalinclude} ../../docs_src/coiled/coiled_functions.py
```

To execute the workflow, you need to turn on parallelization by requesting two or more
workers or specifying one of the parallel backends. Otherwise, the decorated task is run
locally.

```console
pytask -n 2
pytask --parallel-backend loky
```

```{note}
When you build a project using coiled, you will see a message after pytask's startup
that coiled is creating the remote software environment which takes 1-2m.
```

When you apply the {func}`@task <pytask.task>` decorator to the task, make sure the
{func}`@coiled.function <coiled.function>` decorator is applied first or is closer to
the function. Otherwise, it will be ignored. Add more arguments to the decorator to
configure the hardware and software environment.

```{literalinclude} ../../docs_src/coiled/coiled_functions_task.py
```

By default, {func}`@coiled.function <coiled.function>`
[scales adaptively](https://docs.coiled.io/user_guide/usage/functions/index.html#adaptive-scaling)
to the workload. It means that coiled infers from the number of submitted tasks and
previous runtimes, how many additional remote workers it should deploy to handle the
workload. It provides a convenient mechanism to scale without intervention. Also,
workers launched by {func}`@coiled.function <coiled.function>` will shut down quicker
than a cluster.

```{seealso}
Serverless functions are more thoroughly explained in
[coiled's guide](https://docs.coiled.io/user_guide/usage/functions/index.html).
```

(coiled-clusters)=

## Running a cluster

It is also possible to launch a cluster and run each task in a worker provided by
coiled. Usually, it is not necessary and you are better off using coiled's serverless
functions.

If you want to launch a cluster managed by coiled, register a function that builds an
executor using {class}`coiled.Cluster`. Assign a name to the cluster to reuse it when
you build your project again and the cluster has not been shut down.

```python
import coiled
from pytask_parallel import ParallelBackend
from pytask_parallel import registry
from concurrent.futures import Executor


def _build_coiled_executor(n_workers: int) -> Executor:
    return (
        coiled.Cluster(n_workers=n_workers, name="coiled-project")
        .get_client()
        .get_executor()
    )


registry.register_parallel_backend(ParallelBackend.CUSTOM, _build_coiled_executor)
```

Then, execute your workflow with

```console
pytask --parallel-backend custom
```

## Tips

When you are changing your project during executions and your cluster is still up and
running, the local and the remote software environment can get out of sync. Then, you
see errors in remote workers you have fixed locally.

A quick solution is to stop the cluster in the coiled dashboard and create a new one
with the next `pytask build`.
