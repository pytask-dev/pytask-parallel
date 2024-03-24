"""Configures the available backends."""

from __future__ import annotations

import enum
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

import cloudpickle


def deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
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
            deserialize_and_run_with_cloudpickle,
            fn=cloudpickle.dumps(fn),
            kwargs=cloudpickle.dumps(kwargs),
        )


try:
    from loky import get_reusable_executor

except ImportError:

    class ParallelBackend(enum.Enum):
        """Choices for parallel backends."""

        PROCESSES = "processes"
        THREADS = "threads"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackend.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackend.PROCESSES: CloudpickleProcessPoolExecutor,
        ParallelBackend.THREADS: ThreadPoolExecutor,
    }

else:

    class ParallelBackend(enum.Enum):  # type: ignore[no-redef]
        """Choices for parallel backends."""

        PROCESSES = "processes"
        THREADS = "threads"
        LOKY = "loky"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackend.LOKY  # type: ignore[attr-defined]

    PARALLEL_BACKENDS = {
        ParallelBackend.PROCESSES: CloudpickleProcessPoolExecutor,
        ParallelBackend.THREADS: ThreadPoolExecutor,
        ParallelBackend.LOKY: get_reusable_executor,  # type: ignore[attr-defined]
    }
