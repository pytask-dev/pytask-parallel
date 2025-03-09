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
from typing import ClassVar

import cloudpickle
from attrs import define
from loky import get_reusable_executor

__all__ = ["ParallelBackend", "ParallelBackendRegistry", "WorkerType", "registry"]


def _deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class _CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """Patches the standard executor to serialize functions with cloudpickle."""

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,  # noqa: ARG002
        **kwargs: Any,
    ) -> Future[Any]:
        """Submit a new task."""
        return super().submit(
            _deserialize_and_run_with_cloudpickle,
            fn=cloudpickle.dumps(fn),
            kwargs=cloudpickle.dumps(kwargs),
        )


def _get_dask_executor(n_workers: int) -> Executor:
    """Get an executor from a dask client."""
    _rich_traceback_guard = True
    from pytask import import_optional_dependency

    distributed = import_optional_dependency("distributed")
    assert distributed  # noqa: S101
    try:
        client = distributed.Client.current()
    except ValueError:
        client = distributed.Client(distributed.LocalCluster(n_workers=n_workers))
    else:
        if client.cluster and len(client.cluster.workers) != n_workers:
            warnings.warn(
                "The number of workers in the dask cluster "
                f"({len(client.cluster.workers)}) does not match the number of workers "
                f"requested ({n_workers}). The requested number of workers will be "
                "ignored.",
                stacklevel=1,
            )
    return client.get_executor()


def _get_loky_executor(n_workers: int) -> Executor:
    """Get a loky executor."""
    return get_reusable_executor(max_workers=n_workers)


def _get_process_pool_executor(n_workers: int) -> Executor:
    """Get a process pool executor."""
    return _CloudpickleProcessPoolExecutor(max_workers=n_workers)


def _get_thread_pool_executor(n_workers: int) -> Executor:
    """Get a thread pool executor."""
    return ThreadPoolExecutor(max_workers=n_workers)


class ParallelBackend(Enum):
    """Choices for parallel backends.

    Attributes
    ----------
    NONE
        No parallel backend.
    CUSTOM
        A custom parallel backend.
    DASK
        A dask parallel backend.
    LOKY
        A loky parallel backend.
    PROCESSES
        A process pool parallel backend.
    THREADS
        A thread pool parallel backend.

    """

    NONE = "none"
    CUSTOM = "custom"
    DASK = "dask"
    LOKY = "loky"
    PROCESSES = "processes"
    THREADS = "threads"


class WorkerType(Enum):
    """A type for workers that either spawned as threads or processes.

    Attributes
    ----------
    THREADS
        Workers are threads.
    PROCESSES
        Workers are processes.

    """

    THREADS = "threads"
    PROCESSES = "processes"


@define
class _ParallelBackend:
    builder: Callable[..., Executor]
    worker_type: WorkerType
    remote: bool


@define
class ParallelBackendRegistry:
    """Registry for parallel backends."""

    registry: ClassVar[dict[ParallelBackend, _ParallelBackend]] = {}

    def register_parallel_backend(
        self,
        kind: ParallelBackend,
        builder: Callable[..., Executor],
        *,
        worker_type: WorkerType | str = WorkerType.PROCESSES,
        remote: bool = False,
    ) -> None:
        """Register a parallel backend."""
        self.registry[kind] = _ParallelBackend(
            builder=builder, worker_type=WorkerType(worker_type), remote=remote
        )

    def get_parallel_backend(self, kind: ParallelBackend, n_workers: int) -> Executor:
        """Get a parallel backend."""
        __tracebackhide__ = True
        try:
            return self.registry[kind].builder(n_workers=n_workers)
        except KeyError:
            msg = f"No registered parallel backend found for kind {kind.value!r}."
            raise ValueError(msg) from None
        except Exception as e:
            msg = f"Could not instantiate parallel backend {kind.value!r}."
            raise ValueError(msg) from e

    def reset(self) -> None:
        """Register the default backends."""
        self.registry.clear()
        for parallel_backend, builder, worker_type, remote in (
            (ParallelBackend.DASK, _get_dask_executor, "processes", False),
            (ParallelBackend.LOKY, _get_loky_executor, "processes", False),
            (ParallelBackend.PROCESSES, _get_process_pool_executor, "processes", False),
            (ParallelBackend.THREADS, _get_thread_pool_executor, "threads", False),
        ):
            self.register_parallel_backend(
                parallel_backend, builder, worker_type=worker_type, remote=remote
            )


registry = ParallelBackendRegistry()
registry.reset()
