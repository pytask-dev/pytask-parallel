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
from pybaum.tree_util import tree_map
from pytask import console
from pytask import ExecutionReport
from pytask import get_marks
from pytask import hookimpl
from pytask import Mark
from pytask import parse_warning_filter
from pytask import remove_internal_traceback_frames_from_exc_info
from pytask import Session
from pytask import Task
from pytask import warning_record_to_str
from pytask import WarningReport
from pytask_parallel.backends import PARALLEL_BACKENDS
from pytask_parallel.backends import ParallelBackendsChoices
from rich.console import ConsoleOptions
from rich.traceback import Traceback


@hookimpl
def pytask_post_parse(config: dict[str, Any]) -> None:
    """Register the parallel backend."""
    if config["parallel_backend"] in (
        ParallelBackendsChoices.LOKY,  # type: ignore[attr-defined]
        ParallelBackendsChoices.PROCESSES,
    ):
        config["pm"].register(ProcessesNameSpace)
    elif config["parallel_backend"] in (ParallelBackendsChoices.THREADS,):
        config["pm"].register(DefaultBackendNameSpace)


@hookimpl(tryfirst=True)
def pytask_execute_build(session: Session) -> bool | None:
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

            session.executor = executor
            sleeper = _Sleeper()

            while session.scheduler.is_active():

                try:
                    newly_collected_reports = []
                    n_new_tasks = session.config["n_workers"] - len(running_tasks)

                    if n_new_tasks >= 1:
                        ready_tasks = list(session.scheduler.get_ready(n_new_tasks))
                    else:
                        ready_tasks = []

                    for task_name in ready_tasks:
                        task = session.dag.nodes[task_name]["task"]
                        session.hook.pytask_execute_task_log_start(
                            session=session, task=task
                        )
                        try:
                            session.hook.pytask_execute_task_setup(
                                session=session, task=task
                            )
                        except Exception:
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
                                except Exception:
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
                    else:
                        sleeper.sleep()
                except KeyboardInterrupt:
                    break

        return True
    return None


def _parse_future_exception(
    exception: BaseException | None,
) -> tuple[type[BaseException], BaseException, TracebackType] | None:
    """Parse a future exception."""
    return (
        None
        if exception is None
        else (type(exception), exception, exception.__traceback__)
    )


class ProcessesNameSpace:
    """The name space for hooks related to processes."""

    @staticmethod
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session: Session, task: Task) -> Future[Any] | None:
        """Execute a task.

        Take a task, pickle it and send the bytes over to another process.

        """
        if session.config["n_workers"] > 1:
            kwargs = _create_kwargs_for_task(task)

            bytes_function = cloudpickle.dumps(task)
            bytes_kwargs = cloudpickle.dumps(kwargs)

            return session.executor.submit(
                _unserialize_and_execute_task,
                bytes_function=bytes_function,
                bytes_kwargs=bytes_kwargs,
                show_locals=session.config["show_locals"],
                console_options=console.options,
                session_filterwarnings=session.config["filterwarnings"],
                task_filterwarnings=get_marks(task, "filterwarnings"),
                task_short_name=task.short_name,
            )
        return None


def _unserialize_and_execute_task(
    bytes_function: bytes,
    bytes_kwargs: bytes,
    show_locals: bool,
    console_options: ConsoleOptions,
    session_filterwarnings: tuple[str, ...],
    task_filterwarnings: tuple[Mark, ...],
    task_short_name: str,
) -> tuple[list[WarningReport], tuple[type[BaseException], BaseException, str] | None]:
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute in a
    spawned process or thread.

    """
    __tracebackhide__ = True

    task = cloudpickle.loads(bytes_function)
    kwargs = cloudpickle.loads(bytes_kwargs)

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
            task.execute(**kwargs)
        except Exception:
            exc_info = sys.exc_info()
            processed_exc_info = _process_exception(
                exc_info, show_locals, console_options
            )
        else:
            processed_exc_info = None

        warning_reports = []
        for warning_message in log:
            fs_location = warning_message.filename, warning_message.lineno
            warning_reports.append(
                WarningReport(
                    message=warning_record_to_str(warning_message),
                    fs_location=fs_location,
                    id_=task_short_name,
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
            return session.executor.submit(
                _mock_processes_for_threads, func=task.execute, **kwargs
            )
        else:
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
    except Exception:
        exc_info = sys.exc_info()
    else:
        exc_info = None
    return [], exc_info


def _create_kwargs_for_task(task: Task) -> dict[Any, Any]:
    """Create kwargs for task function."""
    kwargs = {**task.kwargs}

    func_arg_names = set(inspect.signature(task.function).parameters)
    for arg_name in ("depends_on", "produces"):
        if arg_name in func_arg_names:
            attribute = getattr(task, arg_name)
            kwargs[arg_name] = tree_map(lambda x: x.value, attribute)

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
