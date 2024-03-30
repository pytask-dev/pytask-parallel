"""Configures the available backends."""

from __future__ import annotations

from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any
from typing import Callable
from typing import ClassVar

import cloudpickle
from loky import get_reusable_executor
from pytask import import_optional_dependency

__all__ = ["ParallelBackend", "ParallelBackendRegistry", "registry"]


def _deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class _CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
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
            _deserialize_and_run_with_cloudpickle,
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

    CUSTOM = "custom"
    LOKY = "loky"
    PROCESSES = "processes"
    THREADS = "threads"


class ParallelBackendRegistry:
    """Registry for parallel backends."""

    registry: ClassVar[dict[ParallelBackend, Callable[..., Executor]]] = {}

    def register_parallel_backend(
        self, kind: ParallelBackend, builder: Callable[..., Executor]
    ) -> None:
        """Register a parallel backend."""
        self.registry[kind] = builder

    def get_parallel_backend(self, kind: ParallelBackend, n_workers: int) -> Executor:
        """Get a parallel backend."""
        __tracebackhide__ = True
        try:
            return self.registry[kind](n_workers=n_workers)
        except KeyError:
            msg = f"No registered parallel backend found for kind {kind}."
            raise ValueError(msg) from None
        except Exception as e:  # noqa: BLE001
            msg = f"Could not instantiate parallel backend {kind.value}."
            raise ValueError(msg) from e


registry = ParallelBackendRegistry()


registry.register_parallel_backend(
    ParallelBackend.PROCESSES,
    lambda n_workers: _CloudpickleProcessPoolExecutor(max_workers=n_workers),
)
registry.register_parallel_backend(
    ParallelBackend.THREADS, lambda n_workers: ThreadPoolExecutor(max_workers=n_workers)
)
registry.register_parallel_backend(
    ParallelBackend.LOKY, lambda n_workers: get_reusable_executor(max_workers=n_workers)
)
