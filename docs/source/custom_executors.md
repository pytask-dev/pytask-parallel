# Custom Executors

```{note}
Please, consider contributing your executor to pytask-parallel if you believe it
could be helpful to other people. Start by creating an issue or a draft PR.
```

pytask-parallel allows you to use any parallel backend as long as it follows the
interface defined by {class}`concurrent.futures.Executor`.

In some cases, adding a new backend can be as easy as registering a builder function
that receives `n_workers` and returns the instantiated executor.

```{important}
Place the code in any module that will be imported when you are executing pytask.
For example, the `src/project/config.py` in your project, the `src/project/__init__.py`
or the task module directly.
```

```{literalinclude} ../../docs_src/custom_executors.py
```

Given the optional {class}`~pytask_parallel.WorkerType` pytask applies automatic
wrappers around the task function to collect tracebacks, capture stdout/stderr and their
like. Possible values are `WorkerType.PROCESSES` (default) or `WorkerType.THREADS`.

The `remote` keyword signals pytask that tasks are executed in remote workers without
access to the local filesytem. pytask will then automatically sync local files to the
workers. By default, pytask assumes workers have access to the local filesytem.

Now, build the project with your custom backend.

```console
pytask --parallel-backend custom
```
