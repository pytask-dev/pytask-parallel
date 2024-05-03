# Developer's Guide

`pytask-parallel` does not call the `pytask_execute_task_protocol` hook
specification/entry-point because `pytask_execute_task_setup` and `pytask_execute_task`
need to be separated from `pytask_execute_task_teardown`. Thus, plugins that change this
hook specification may not interact well with the parallelization.

Two PRs for CPython try to re-enable setting custom reducers which should have been
working but does not. Here are the references.

- https://bugs.python.org/issue28053
- https://github.com/python/cpython/pull/9959
- https://github.com/python/cpython/pull/15058
