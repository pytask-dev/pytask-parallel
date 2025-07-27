from concurrent.futures import Executor

from my_project.executor import CustomExecutor  # ty: ignore[unresolved-import]

from pytask_parallel import ParallelBackend
from pytask_parallel import WorkerType
from pytask_parallel import registry


def build_custom_executor(n_workers: int) -> Executor:
    return CustomExecutor(max_workers=n_workers)


registry.register_parallel_backend(
    ParallelBackend.CUSTOM,
    build_custom_executor,
    # Optional defaults.
    worker_type=WorkerType.PROCESSES,
    remote=False,
)
