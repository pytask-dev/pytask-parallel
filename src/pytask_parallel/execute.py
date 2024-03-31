"""Contains code relevant to the execution."""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING
from typing import Any

from attrs import define
from attrs import field
from pytask import ExecutionReport
from pytask import PNode
from pytask import PTask
from pytask import PythonNode
from pytask import Session
from pytask import hookimpl
from pytask.tree_util import PyTree
from pytask.tree_util import tree_map
from pytask.tree_util import tree_structure

from pytask_parallel.backends import registry
from pytask_parallel.utils import parse_future_result

if TYPE_CHECKING:
    from concurrent.futures import Future


@hookimpl
def pytask_execute_build(session: Session) -> bool | None:  # noqa: C901, PLR0915
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

    parallel_backend = registry.get_parallel_backend(
        session.config["parallel_backend"], n_workers=session.config["n_workers"]
    )

    with parallel_backend as executor:
        session.config["_parallel_executor"] = executor
        sleeper = _Sleeper()

        i = 0
        while session.scheduler.is_active():
            try:
                newly_collected_reports = []
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
                    except Exception:  # noqa: BLE001
                        report = ExecutionReport.from_task_and_exception(
                            task, sys.exc_info()
                        )
                        newly_collected_reports.append(report)
                        session.scheduler.done(task_name)
                    else:
                        running_tasks[task_name] = session.hook.pytask_execute_task(
                            session=session, task=task
                        )
                        sleeper.reset()

                if not ready_tasks:
                    sleeper.increment()

                for task_name in list(running_tasks):
                    future = running_tasks[task_name]

                    if future.done():
                        python_nodes, warnings_reports, exc_info = parse_future_result(
                            future
                        )
                        session.warnings.extend(warnings_reports)

                        if exc_info is not None:
                            task = session.dag.nodes[task_name]["task"]
                            newly_collected_reports.append(
                                ExecutionReport.from_task_and_exception(task, exc_info)
                            )
                            running_tasks.pop(task_name)
                            session.scheduler.done(task_name)
                        else:
                            task = session.dag.nodes[task_name]["task"]
                            _update_python_nodes(task, python_nodes)

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


def _update_python_nodes(
    task: PTask, python_nodes: dict[str, PyTree[PythonNode | None]] | None
) -> None:
    """Update the python nodes of a task with the python nodes from the future."""

    def _update_python_node(x: PNode, y: PythonNode | None) -> PNode:
        if y:
            x.save(y.load())
        return x

    structure_python_nodes = tree_structure(python_nodes)
    structure_produces = tree_structure(task.produces)
    # strict must be false when none is leaf.
    if structure_produces.is_prefix(structure_python_nodes, strict=False):
        task.produces = tree_map(_update_python_node, task.produces, python_nodes)  # type: ignore[assignment]


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
