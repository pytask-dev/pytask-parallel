"""Contains code relevant to the execution."""
import sys
import time
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Type

import cloudpickle
from _pytask.config import hookimpl
from _pytask.console import console
from _pytask.report import ExecutionReport
from _pytask.traceback import remove_internal_traceback_frames_from_exc_info
from pytask import MetaTask
from pytask import Session
from pytask_parallel.backends import PARALLEL_BACKENDS
from rich.console import ConsoleOptions
from rich.traceback import Traceback


@hookimpl
def pytask_post_parse(config: Dict[str, Any]) -> None:
    """Register the parallel backend."""
    if config["parallel_backend"] in ["loky", "processes"]:
        config["pm"].register(ProcessesNameSpace)
    elif config["parallel_backend"] in ["threads"]:
        config["pm"].register(DefaultBackendNameSpace)


@hookimpl(tryfirst=True)
def pytask_execute_build(session: Session) -> Optional[bool]:
    """Execute tasks with a parallel backend.

    There are three phases while the scheduler has tasks which need to be executed.

    1. Take all ready tasks, set up their execution and submit them.
    2. For all tasks which are running, find those which have finished and turn them
       into a report.
    3. Process all reports and report the result on the command line.

    """
    if session.config["n_workers"] > 1:
        reports = session.execution_reports
        running_tasks: Dict[str, Future[Any]] = {}

        parallel_backend = PARALLEL_BACKENDS[session.config["parallel_backend"]]

        with parallel_backend(max_workers=session.config["n_workers"]) as executor:

            session.executor = executor

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

                    for task_name in list(running_tasks):
                        future = running_tasks[task_name]
                        if future.done() and (
                            future.exception() is not None
                            or future.result() is not None
                        ):
                            task = session.dag.nodes[task_name]["task"]
                            if future.exception() is not None:
                                exception = future.exception()
                                exc_info = (
                                    type(exception),
                                    exception,
                                    exception.__traceback__,
                                )
                            else:
                                exc_info = future.result()

                            newly_collected_reports.append(
                                ExecutionReport.from_task_and_exception(task, exc_info)
                            )
                            running_tasks.pop(task_name)
                            session.scheduler.done(task_name)
                        elif future.done() and future.exception() is None:
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
                        time.sleep(session.config["delay"])
                except KeyboardInterrupt:
                    break

        return True
    return None


class ProcessesNameSpace:
    """The name space for hooks related to processes."""

    @staticmethod
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session: Session, task: MetaTask) -> Optional[Future[Any]]:
        """Execute a task.

        Take a task, pickle it and send the bytes over to another process.

        """
        if session.config["n_workers"] > 1:
            bytes_ = cloudpickle.dumps(task)
            return session.executor.submit(
                _unserialize_and_execute_task,
                bytes_=bytes_,
                show_locals=session.config["show_locals"],
                console_options=console.options,
            )
        return None


def _unserialize_and_execute_task(
    bytes_: bytes, show_locals: bool, console_options: ConsoleOptions
) -> Optional[Tuple[Type[BaseException], BaseException, str]]:
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute
    in a spawned process or thread.

    """
    __tracebackhide__ = True

    task = cloudpickle.loads(bytes_)

    try:
        task.execute()
    except Exception:
        exc_info = sys.exc_info()
        processed_exc_info = _process_exception(exc_info, show_locals, console_options)
        return processed_exc_info
    return None


def _process_exception(
    exc_info: Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
    show_locals: bool,
    console_options: ConsoleOptions,
) -> Tuple[Type[BaseException], BaseException, str]:
    exc_info = remove_internal_traceback_frames_from_exc_info(exc_info)
    traceback = Traceback.from_exception(*exc_info, show_locals=show_locals)
    segments = console.render(traceback, options=console_options)
    text = "".join(segment.text for segment in segments)
    return (*exc_info[:2], text)


class DefaultBackendNameSpace:
    """The name space for hooks related to threads."""

    @staticmethod
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session: Session, task: MetaTask) -> Optional[Future[Any]]:
        """Execute a task.

        Since threads have shared memory, it is not necessary to pickle and unpickle the
        task.

        """
        if session.config["n_workers"] > 1:
            return session.executor.submit(task.execute)
        return None
