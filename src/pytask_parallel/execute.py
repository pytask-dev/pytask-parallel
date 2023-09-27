"""Contains code relevant to the execution."""
from __future__ import annotations

import inspect
import sys
import time
import warnings
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import List

import attr
import cloudpickle
from pytask import console
from pytask import ExecutionReport
from pytask import get_marks
from pytask import hookimpl
from pytask import Mark
from pytask import parse_warning_filter
from pytask import PTask
from pytask import remove_internal_traceback_frames_from_exc_info
from pytask import Session
from pytask import Task
from pytask import warning_record_to_str
from pytask import WarningReport
from pytask.tree_util import PyTree
from pytask.tree_util import tree_leaves
from pytask.tree_util import tree_map
from pytask.tree_util import tree_structure
from pytask_parallel.backends import PARALLEL_BACKENDS
from pytask_parallel.backends import ParallelBackend
from rich.console import ConsoleOptions
from rich.traceback import Traceback


@hookimpl
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Register the parallel backend."""
    if config["parallel_backend"] == ParallelBackend.THREADS:
        config["pm"].register(DefaultBackendNameSpace)
    else:
        config["pm"].register(ProcessesNameSpace)


@hookimpl(tryfirst=True)
def pytask_execute_build(session: Session) -> bool | None:  # noqa: C901, PLR0915
    """Execute tasks with a parallel backend.

    There are three phases while the scheduler has tasks which need to be executed.

    1. Take all ready tasks, set up their execution and submit them.
    2. For all tasks which are running, find those which have finished and turn them
       into a report.
    3. Process all reports and report the result on the command line.

    """
    if session.config["n_workers"] > 1:
        reports = session.execution_reports
        running_tasks: dict[str, Future[Any]] = {}

        parallel_backend = PARALLEL_BACKENDS[session.config["parallel_backend"]]

        with parallel_backend(max_workers=session.config["n_workers"]) as executor:
            session.config["_parallel_executor"] = executor
            sleeper = _Sleeper()

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
                            # An exception was thrown before the task was executed.
                            if future.exception() is not None:
                                exc_info = _parse_future_exception(future.exception())
                                warning_reports = []
                            # A task raised an exception.
                            else:
                                warning_reports, task_exception = future.result()
                                session.warnings.extend(warning_reports)
                                exc_info = (
                                    _parse_future_exception(future.exception())
                                    or task_exception
                                )

                            if exc_info is not None:
                                task = session.dag.nodes[task_name]["task"]
                                newly_collected_reports.append(
                                    ExecutionReport.from_task_and_exception(
                                        task, exc_info
                                    )
                                )
                                running_tasks.pop(task_name)
                                session.scheduler.done(task_name)
                            else:
                                task = session.dag.nodes[task_name]["task"]
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
                        else:
                            pass

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

        return True
    return None


def _parse_future_exception(
    exc: BaseException | None,
) -> tuple[type[BaseException], BaseException, TracebackType] | None:
    """Parse a future exception into the format of ``sys.exc_info``."""
    return None if exc is None else (type(exc), exc, exc.__traceback__)


class ProcessesNameSpace:
    """The name space for hooks related to processes."""

    @staticmethod
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session: Session, task: PTask) -> Future[Any] | None:
        """Execute a task.

        Take a task, pickle it and send the bytes over to another process.

        """
        if session.config["n_workers"] > 1:
            kwargs = _create_kwargs_for_task(task)

            # Task modules are dynamically loaded and added to `sys.modules`. Thus,
            # cloudpickle believes the module of the task function is also importable in
            # the child process. We have to register the module as dynamic again, so
            # that cloudpickle will pickle it with the function. See cloudpickle#417,
            # pytask#373 and pytask#374.
            task_module = inspect.getmodule(task.function)
            cloudpickle.register_pickle_by_value(task_module)

            return session.config["_parallel_executor"].submit(
                _execute_task,
                task=task,
                kwargs=kwargs,
                show_locals=session.config["show_locals"],
                console_options=console.options,
                session_filterwarnings=session.config["filterwarnings"],
                task_filterwarnings=get_marks(task, "filterwarnings"),
            )
        return None


def _execute_task(  # noqa: PLR0913
    task: PTask,
    kwargs: dict[str, Any],
    show_locals: bool,
    console_options: ConsoleOptions,
    session_filterwarnings: tuple[str, ...],
    task_filterwarnings: tuple[Mark, ...],
) -> tuple[list[WarningReport], tuple[type[BaseException], BaseException, str] | None]:
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute in a
    spawned process or thread.

    """
    __tracebackhide__ = True

    with warnings.catch_warnings(record=True) as log:
        # mypy can't infer that record=True means log is not None; help it.
        assert log is not None

        for arg in session_filterwarnings:
            warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        # apply filters from "filterwarnings" marks
        for mark in task_filterwarnings:
            for arg in mark.args:
                warnings.filterwarnings(*parse_warning_filter(arg, escape=False))

        try:
            out = task.execute(**kwargs)
        except Exception:  # noqa: BLE001
            exc_info = sys.exc_info()
            processed_exc_info = _process_exception(
                exc_info, show_locals, console_options
            )
        else:
            if "return" in task.produces:
                structure_out = tree_structure(out)
                structure_return = tree_structure(task.produces["return"])
                # strict must be false when none is leaf.
                if not structure_return.is_prefix(structure_out, strict=False):
                    msg = (
                        "The structure of the return annotation is not a subtree of "
                        "the structure of the function return.\n\nFunction return: "
                        f"{structure_out}\n\nReturn annotation: {structure_return}"
                    )
                    raise ValueError(msg)

                nodes = tree_leaves(task.produces["return"])
                values = structure_return.flatten_up_to(out)
                for node, value in zip(nodes, values):
                    node.save(value)  # type: ignore[attr-defined]

            processed_exc_info = None

        task_display_name = getattr(task, "display_name", task.name)
        warning_reports = []
        for warning_message in log:
            fs_location = warning_message.filename, warning_message.lineno
            warning_reports.append(
                WarningReport(
                    message=warning_record_to_str(warning_message),
                    fs_location=fs_location,
                    id_=task_display_name,
                )
            )

    return warning_reports, processed_exc_info


def _process_exception(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None],
    show_locals: bool,
    console_options: ConsoleOptions,
) -> tuple[type[BaseException], BaseException, str]:
    """Process the exception and convert the traceback to a string."""
    exc_info = remove_internal_traceback_frames_from_exc_info(exc_info)
    traceback = Traceback.from_exception(*exc_info, show_locals=show_locals)
    segments = console.render(traceback, options=console_options)
    text = "".join(segment.text for segment in segments)
    return (*exc_info[:2], text)


class DefaultBackendNameSpace:
    """The name space for hooks related to threads."""

    @staticmethod
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session: Session, task: Task) -> Future[Any] | None:
        """Execute a task.

        Since threads have shared memory, it is not necessary to pickle and unpickle the
        task.

        """
        if session.config["n_workers"] > 1:
            kwargs = _create_kwargs_for_task(task)
            return session.config["_parallel_executor"].submit(
                _mock_processes_for_threads, func=task.execute, **kwargs
            )
        return None


def _mock_processes_for_threads(
    func: Callable[..., Any], **kwargs: Any
) -> tuple[list[Any], tuple[type[BaseException], BaseException, TracebackType] | None]:
    """Mock execution function such that it returns the same as for processes.

    The function for processes returns ``warning_reports`` and an ``exception``. With
    threads, these object are collected by the main and not the subprocess. So, we just
    return placeholders.

    """
    __tracebackhide__ = True
    try:
        func(**kwargs)
    except Exception:  # noqa: BLE001
        exc_info = sys.exc_info()
    else:
        exc_info = None
    return [], exc_info


def _create_kwargs_for_task(task: PTask) -> dict[str, PyTree[Any]]:
    """Create kwargs for task function."""
    parameters = inspect.signature(task.function).parameters

    kwargs = {}
    for name, value in task.depends_on.items():
        kwargs[name] = tree_map(lambda x: x.load(), value)

    for name, value in task.produces.items():
        if name in parameters:
            kwargs[name] = tree_map(lambda x: x.load(), value)

    return kwargs


@attr.s(kw_only=True)
class _Sleeper:
    """A sleeper that always sleeps a bit and up to 1 second if you don't wake it up.

    This class controls when the next iteration of the execution loop starts. If new
    tasks are scheduled, the time spent sleeping is reset to a lower value.

    """

    timings = attr.ib(type=List[float], default=[(i / 10) ** 2 for i in range(1, 11)])
    timing_idx = attr.ib(type=int, default=0)

    def reset(self) -> None:
        self.timing_idx = 0

    def increment(self) -> None:
        if self.timing_idx < len(self.timings) - 1:
            self.timing_idx += 1

    def sleep(self) -> None:
        time.sleep(self.timings[self.timing_idx])
