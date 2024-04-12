# coiled

[coiled](https://www.coiled.io/) is a product built on top of dask that eases the
deployment of your workflow to many cloud providers like AWS, GCP, and Azure.

Note that, coiled is a paid service. They offer a
[free monthly tier](https://www.coiled.io/pricing) where you only need to pay the costs
for your cloud provider and you can get started without a credit card.

They provide the following benefits which are especially helpful to people who are not
familiar with cloud providers or remote computing.

- coiled manages your resources by spawning workers if you need them and shutting them
  down if they are idle.
- Synchronization of your local environment to remote workers.

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

When you apply the {func}`@task <pytask.task>` decorator to the task, make sure the
`@coiled.function` decorator is applied first, or is closer to the function. Otherwise,
it will be ignored.

```{literalinclude} ../../docs_src/coiled/coiled_functions_task.py
```

```{seealso}
Serverless functions are more thoroughly explained in
[coiled's guide](https://docs.coiled.io/user_guide/usage/functions/index.html).
```

(coiled-clusters)=

## Running a cluster

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
