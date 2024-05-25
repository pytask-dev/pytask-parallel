"""Contains code relevant to the execution."""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING
from typing import Any

import cloudpickle
from _pytask.node_protocols import PPathNode
from attrs import define
from attrs import field
from pytask import ExecutionReport
from pytask import PNode
from pytask import PTask
from pytask import PythonNode
from pytask import Session
from pytask import console
from pytask import get_marks
from pytask import hookimpl
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map
from pytask.tree_util import tree_structure

from pytask_parallel.backends import WorkerType
from pytask_parallel.backends import registry
from pytask_parallel.typing import CarryOverPath
from pytask_parallel.typing import is_coiled_function
from pytask_parallel.utils import create_kwargs_for_task
from pytask_parallel.utils import get_module
from pytask_parallel.utils import parse_future_result

if TYPE_CHECKING:
    from concurrent.futures import Future

    from pytask_parallel.wrappers import WrapperResult


@hookimpl
def pytask_execute_build(session: Session) -> bool | None:  # noqa: C901, PLR0912, PLR0915
    """Execute tasks with a parallel backend.

    There are three phases while the scheduler has tasks which need to be executed.

    1. Take all ready tasks, set up their execution and submit them.
    2. For all tasks which are running, find those which have finished and turn them
       into a report.
    3. Process all reports and report the result on the command line.

    """
    __tracebackhide__ = True
    reports = session.execution_reports
    running_tasks: dict[str, Future[Any]] = {}
    any_coiled_task = any(is_coiled_function(task) for task in session.tasks)

    # The executor can only be created after the collection to give users the
    # possibility to inject their own executors.
    session.config["_parallel_executor"] = registry.get_parallel_backend(
        session.config["parallel_backend"], n_workers=session.config["n_workers"]
    )

    with session.config["_parallel_executor"]:
        sleeper = _Sleeper()

        i = 0
        while session.scheduler.is_active():
            try:
                newly_collected_reports = []

                # If there is any coiled function, the user probably wants to exploit
                # adaptive scaling. Thus, we need to submit all ready tasks.
                # Unfortunately, all submitted tasks are shown as running although some
                # are pending.
                #
                # Without coiled functions, we submit as many tasks as there are
                # available workers since we cannot reliably detect a pending status.
                #
                # See #98 for more information.
                if any_coiled_task:
                    n_new_tasks = 10_000
                else:
                    n_new_tasks = session.config["n_workers"] - len(running_tasks)

                ready_tasks = (
                    list(session.scheduler.get_ready(n_new_tasks))
                    if n_new_tasks >= 1
                    else []
                )

                for task_name in ready_tasks:
                    task = session.dag.nodes[task_name]["task"]
                    session.hook.pytask_execute_task_log_start(
                        session=session, task=task
                    )
                    try:
                        session.hook.pytask_execute_task_setup(
                            session=session, task=task
                        )
                        running_tasks[task_name] = session.hook.pytask_execute_task(
                            session=session, task=task
                        )
                        sleeper.reset()
                    except Exception:  # noqa: BLE001
                        report = ExecutionReport.from_task_and_exception(
                            task, sys.exc_info()
                        )
                        newly_collected_reports.append(report)
                        session.scheduler.done(task_name)

                if not ready_tasks:
                    sleeper.increment()

                for task_name in list(running_tasks):
                    future = running_tasks[task_name]

                    if future.done():
                        wrapper_result = parse_future_result(future)
                        session.warnings.extend(wrapper_result.warning_reports)

                        if wrapper_result.stdout:
                            task.report_sections.append(
                                ("call", "stdout", wrapper_result.stdout)
                            )
                        if wrapper_result.stderr:
                            task.report_sections.append(
                                ("call", "stderr", wrapper_result.stderr)
                            )

                        if wrapper_result.exc_info is not None:
                            task = session.dag.nodes[task_name]["task"]
                            newly_collected_reports.append(
                                ExecutionReport.from_task_and_exception(
                                    task,
                                    wrapper_result.exc_info,  # type: ignore[arg-type]
                                )
                            )
                            running_tasks.pop(task_name)
                            session.scheduler.done(task_name)
                        else:
                            task = session.dag.nodes[task_name]["task"]
                            _update_carry_over_products(
                                task, wrapper_result.carry_over_products
                            )

                            try:
                                session.hook.pytask_execute_task_teardown(
                                    session=session, task=task
                                )
                            except Exception:  # noqa: BLE001
                                report = ExecutionReport.from_task_and_exception(
                                    task, sys.exc_info()
                                )
                            else:
                                report = ExecutionReport.from_task(task)

                            running_tasks.pop(task_name)
                            newly_collected_reports.append(report)
                            session.scheduler.done(task_name)

                for report in newly_collected_reports:
                    session.hook.pytask_execute_task_process_report(
                        session=session, report=report
                    )
                    session.hook.pytask_execute_task_log_end(
                        session=session, task=task, report=report
                    )
                    reports.append(report)

                if session.should_stop:
                    break
                sleeper.sleep()

            except KeyboardInterrupt:
                break

            i += 1

    return True


@hookimpl
def pytask_execute_task(session: Session, task: PTask) -> Future[WrapperResult]:
    """Execute a task.

    The task function is wrapped according to the worker type and submitted to the
    executor.

    """
    parallel_backend = registry.registry[session.config["parallel_backend"]]
    worker_type = parallel_backend.worker_type
    remote = parallel_backend.remote

    kwargs = create_kwargs_for_task(task, remote=remote)

    if is_coiled_function(task):
        # Prevent circular import for coiled backend.
        from pytask_parallel.wrappers import rewrap_task_with_coiled_function

        wrapper_func = rewrap_task_with_coiled_function(task)

        # Task modules are dynamically loaded and added to `sys.modules`. Thus,
        # cloudpickle believes the module of the task function is also importable in the
        # child process. We have to register the module as dynamic again, so that
        # cloudpickle will pickle it with the function. See cloudpickle#417, pytask#373
        # and pytask#374.
        task_module = get_module(task.function, getattr(task, "path", None))
        cloudpickle.register_pickle_by_value(task_module)

        return wrapper_func.submit(
            task=task,
            console_options=console.options,
            kwargs=kwargs,
            remote=True,
            session_filterwarnings=session.config["filterwarnings"],
            show_locals=session.config["show_locals"],
            task_filterwarnings=get_marks(task, "filterwarnings"),
        )

    if worker_type == WorkerType.PROCESSES:
        # Prevent circular import for loky backend.
        from pytask_parallel.wrappers import wrap_task_in_process

        # Task modules are dynamically loaded and added to `sys.modules`. Thus,
        # cloudpickle believes the module of the task function is also importable in the
        # child process. We have to register the module as dynamic again, so that
        # cloudpickle will pickle it with the function. See cloudpickle#417, pytask#373
        # and pytask#374.
        task_module = get_module(task.function, getattr(task, "path", None))
        cloudpickle.register_pickle_by_value(task_module)

        return session.config["_parallel_executor"].submit(
            wrap_task_in_process,
            task=task,
            console_options=console.options,
            kwargs=kwargs,
            remote=remote,
            session_filterwarnings=session.config["filterwarnings"],
            show_locals=session.config["show_locals"],
            task_filterwarnings=get_marks(task, "filterwarnings"),
        )

    if worker_type == WorkerType.THREADS:
        # Prevent circular import for loky backend.
        from pytask_parallel.wrappers import wrap_task_in_thread

        return session.config["_parallel_executor"].submit(
            wrap_task_in_thread, task=task, remote=False, **kwargs
        )

    msg = f"Unknown worker type {worker_type}"
    raise ValueError(msg)


@hookimpl
def pytask_unconfigure() -> None:
    """Clean up the parallel executor."""
    registry.reset()


def _update_carry_over_products(
    task: PTask, carry_over_products: PyTree[CarryOverPath | PythonNode | None] | None
) -> None:
    """Update products carry over from a another process or remote worker.

    The python node can be a regular one passing the value to another python node.

    In other instances the python holds a string or bytes from a RemotePathNode.

    """

    def _update_carry_over_node(
        x: PNode, y: CarryOverPath | PythonNode | None
    ) -> PNode:
        if y is None:
            return x
        if isinstance(x, PPathNode) and isinstance(y, CarryOverPath):
            x.path.write_bytes(y.content)
            return x
        if isinstance(y, PythonNode):
            x.save(y.load())
            return x
        raise NotImplementedError

    structure_carry_over_products = tree_structure(carry_over_products)
    structure_produces = tree_structure(task.produces)
    # strict must be false when none is leaf.
    if structure_produces.is_prefix(structure_carry_over_products, strict=False):
        task.produces = tree_map(
            _update_carry_over_node, task.produces, carry_over_products
        )  # type: ignore[assignment]


@define(kw_only=True)
class _Sleeper:
    """A sleeper that always sleeps a bit and up to 1 second if you don't wake it up.

    This class controls when the next iteration of the execution loop starts. If new
    tasks are scheduled, the time spent sleeping is reset to a lower value.

    """

    timings: list[float] = field(default=[(i / 10) ** 2 for i in range(1, 11)])
    timing_idx: int = 0

    def reset(self) -> None:
        self.timing_idx = 0

    def increment(self) -> None:
        if self.timing_idx < len(self.timings) - 1:
            self.timing_idx += 1

    def sleep(self) -> None:
        time.sleep(self.timings[self.timing_idx])
