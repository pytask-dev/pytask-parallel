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

__all__ = ["ParallelBackend", "ParallelBackendRegistry", "registry"]


def _deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class _CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """Patches the standard executor to serialize functions with cloudpickle."""

    # The type signature is wrong for version above Py3.7. Fix when 3.7 is deprecated.
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
