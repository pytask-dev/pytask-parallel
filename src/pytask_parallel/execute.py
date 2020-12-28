"""Contains code relevant to the execution."""
import sys
import time

import cloudpickle
from _pytask.config import hookimpl
from _pytask.report import ExecutionReport
from pytask_parallel.backends import PARALLEL_BACKENDS


@hookimpl
def pytask_post_parse(config):
    """Register the parallel backend."""
    if config["parallel_backend"] == "processes":
        config["pm"].register(ProcessesNameSpace)
    elif config["parallel_backend"] in ["threads", "loky"]:
        config["pm"].register(DefaultBackendNameSpace)


@hookimpl(tryfirst=True)
def pytask_execute_build(session):
    """Execute tasks with a parallel backend.

    There are three phases while the scheduler has tasks which need to be executed.

    1. Take all ready tasks, set up their execution and submit them.
    2. For all tasks which are running, find those which have finished and turn them
       into a report.
    3. Process all reports and report the result on the command line.

    """
    if session.config["n_workers"] > 1:
        reports = session.execution_reports
        running_tasks = {}

        parallel_backend = PARALLEL_BACKENDS[session.config["parallel_backend"]]

        with parallel_backend(max_workers=session.config["n_workers"]) as executor:

            session.executor = executor

            while session.scheduler.is_active():

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
                    if future.done() and future.exception() is not None:
                        task = session.dag.nodes[task_name]["task"]
                        exception = future.exception()
                        newly_collected_reports.append(
                            ExecutionReport.from_task_and_exception(
                                task, (type(exception), exception, None)
                            )
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

        return True


class ProcessesNameSpace:
    """The name space for hooks related to processes."""

    @hookimpl(tryfirst=True)
    def pytask_execute_task(session, task):  # noqa: N805
        """Execute a task.

        Take a task, pickle it and send the bytes over to another process.

        """
        if session.config["n_workers"] > 1:
            bytes_ = cloudpickle.dumps(task)
            return session.executor.submit(unserialize_and_execute_task, bytes_)


def unserialize_and_execute_task(bytes_):
    """Unserialize and execute task.

    This function receives bytes and unpickles them to a task which is them execute
    in a spawned process or thread.

    """
    task = cloudpickle.loads(bytes_)
    task.execute()


class DefaultBackendNameSpace:
    """The name space for hooks related to threads."""

    @hookimpl(tryfirst=True)
    def pytask_execute_task(session, task):  # noqa: N805
        """Execute a task.

        Since threads have shared memory, it is not necessary to pickle and unpickle the
        task.

        """
        if session.config["n_workers"] > 1:
            return session.executor.submit(task.execute)
