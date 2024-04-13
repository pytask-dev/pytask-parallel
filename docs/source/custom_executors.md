# Custom Executors

```{caution}
The interface for custom executors is rudimentary right now. Please, give some feedback
if you managed to implement a custom executor or have suggestions for improvement.

Please, also consider contributing your executor to pytask-parallel if you believe it
could be helpful to other people. Start by creating an issue or a draft PR.
```

pytask-parallel allows you to use your parallel backend as long as it follows the
interface defined by {class}`~concurrent.futures.Executor`.

In some cases, adding a new backend can be as easy as registering a builder function
that receives some arguments (currently only `n_workers`) and returns the instantiated
executor.

```{literalinclude} ../../docs_src/custom_executors.py
```

Given {class}`pytask_parallel.WorkerType` pytask applies automatic wrappers around the
task function to collect tracebacks, capture stdout/stderr and their like. The `remote`
keyword allows pytask to handle local paths automatically for remote clusters.

Now, build the project requesting your custom backend.

```console
pytask --parallel-backend custom
```

```{important}
pytask applies automatic wrappers
```
