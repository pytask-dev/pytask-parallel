"""This module configures the available backends."""
from __future__ import annotations

import enum
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

import cloudpickle
from _pytask.path import import_path


def deserialize_and_run_with_cloudpickle(
    fn: bytes, kwargs: bytes, kwargs_import_path: bytes
) -> Any:
    """Deserialize and execute a function and keyword arguments."""
    deserialized_kwargs_import_path = cloudpickle.loads(kwargs_import_path)
    if deserialized_kwargs_import_path:
        import_path(**deserialized_kwargs_import_path)

    deserialized_fn = cloudpickle.loads(fn)
    deserialized_kwargs = cloudpickle.loads(kwargs)
    return deserialized_fn(**deserialized_kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """Patches the standard executor to serialize functions with cloudpickle."""

    # The type signature is wrong for version above Py3.7. Fix when 3.7 is deprecated.
    def submit(  # type: ignore[override]
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any  # noqa: ARG002
    ) -> Future[Any]:
        """Submit a new task."""
        return super().submit(
            deserialize_and_run_with_cloudpickle,
            fn=cloudpickle.dumps(fn),
            kwargs_import_path=cloudpickle.dumps(kwargs.pop("kwargs_import_path")),
            kwargs=cloudpickle.dumps(kwargs),
        )


try:
    from loky import get_reusable_executor

except ImportError:

    class ParallelBackendChoices(enum.Enum):
        """Choices for parallel backends."""

        PROCESSES = "processes"
        THREADS = "threads"

    PARALLEL_BACKENDS = {
        ParallelBackendChoices.PROCESSES: CloudpickleProcessPoolExecutor,
        ParallelBackendChoices.THREADS: ThreadPoolExecutor,
    }

else:

    class ParallelBackendChoices(enum.Enum):  # type: ignore[no-redef]
        """Choices for parallel backends."""

        PROCESSES = "processes"
        THREADS = "threads"
        LOKY = "loky"

    PARALLEL_BACKENDS_DEFAULT = ParallelBackendChoices.PROCESSES

    PARALLEL_BACKENDS = {
        ParallelBackendChoices.PROCESSES: CloudpickleProcessPoolExecutor,
        ParallelBackendChoices.THREADS: ThreadPoolExecutor,
        ParallelBackendChoices.LOKY: (  # type: ignore[attr-defined]
            get_reusable_executor
        ),
    }

PARALLEL_BACKENDS_DEFAULT = ParallelBackendChoices.PROCESSES
