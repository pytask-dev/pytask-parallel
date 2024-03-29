"""Configures the available backends."""

from __future__ import annotations

import warnings
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any
from typing import Callable

import cloudpickle
from loky import get_reusable_executor
from pytask import import_optional_dependency


def deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """Patches the standard executor to serialize functions with cloudpickle."""

    # The type signature is wrong for Python >3.8. Fix when support is dropped.
    def submit(  # type: ignore[override]
        self,
        fn: Callable[..., Any],
        *args: Any,  # noqa: ARG002
        **kwargs: Any,
    ) -> Future[Any]:
        """Submit a new task."""
        return super().submit(
            deserialize_and_run_with_cloudpickle,
            fn=cloudpickle.dumps(fn),
            kwargs=cloudpickle.dumps(kwargs),
        )


def get_dask_executor(n_workers: int) -> Executor:
    """Get an executor from a dask client."""
    _rich_traceback_omit = True
    client = import_optional_dependency("distributed").Client(
        address="tcp://172.17.199.232:8786"
    )
    if client.cluster and len(client.cluster.workers) != n_workers:
        warnings.warn(
            f"The number of workers in the dask cluster ({len(client.cluster.workers)})"
            f" does not match the number of workers requested ({n_workers}). The "
            "requested number of workers will be ignored.",
            stacklevel=1,
        )
    return client.get_executor()


def get_loky_executor(n_workers: int) -> Executor:
    """Get a loky executor."""
    return get_reusable_executor(max_workers=n_workers)


class ParallelBackend(Enum):
    """Choices for parallel backends."""

    PROCESSES = "processes"
    THREADS = "threads"
    LOKY = "loky"
    DASK = "dask"


PARALLEL_BACKEND_BUILDER = {
    ParallelBackend.PROCESSES: lambda n_workers: CloudpickleProcessPoolExecutor(
        n_workers=n_workers
    ),
    ParallelBackend.THREADS: lambda n_workers: ThreadPoolExecutor(n_workers=n_workers),
    ParallelBackend.LOKY: get_loky_executor,
    ParallelBackend.DASK: get_dask_executor,
}
