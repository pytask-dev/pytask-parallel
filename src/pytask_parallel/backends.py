"""Configures the available backends."""

from __future__ import annotations

from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any
from typing import Callable

import cloudpickle
from loky import get_reusable_executor


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


class ParallelBackend(Enum):
    """Choices for parallel backends."""

    PROCESSES = "processes"
    THREADS = "threads"
    LOKY = "loky"


PARALLEL_BACKEND_BUILDER = {
    ParallelBackend.PROCESSES: lambda: CloudpickleProcessPoolExecutor,
    ParallelBackend.THREADS: lambda: ThreadPoolExecutor,
    ParallelBackend.LOKY: lambda: get_reusable_executor,
}
