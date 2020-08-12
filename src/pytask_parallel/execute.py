import sys
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
import networkx as nx
from _pytask.config import hookimpl
from _pytask.report import ExecutionReport
from pytask_parallel.scheduler import TopologicalSorter

PARALLEL_BACKEND = {
    "processes": ProcessPoolExecutor,
    "threads": ThreadPoolExecutor,
}


@hookimpl
def pytask_post_parse(config):
    if config["parallel_backend"] == "processes":
        config["pm"].register(ProcessesNameSpace)
    elif config["parallel_backend"] == "threads":
        config["pm"].register(ThreadsNameSpace)


@hookimpl(tryfirst=True)
def pytask_execute_create_scheduler(session):
    if session.config["n_workers"] > 1:
        task_names = {task.name for task in session.tasks}
        task_dict = {
            name: nx.ancestors(session.dag, name) & task_names for name in task_names
        }
        scheduler = TopologicalSorter(task_dict)

        # Forbid to add further nodes and check for cycles. The latter should have been
        # taken care of while setting up the DAG.
        scheduler.prepare()

        return scheduler


@hookimpl(tryfirst=True)
def pytask_execute_build(session):
    if session.config["n_workers"] > 1:
        reports = []
        running_tasks = {}

        parallel_backend = PARALLEL_BACKEND[session.config["parallel_backend"]]

        with parallel_backend(max_workers=session.config["n_workers"]) as executor:

            session.executor = executor

            while session.scheduler.is_active():

                newly_collected_reports = []
                ready_tasks = list(session.scheduler.get_ready())

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

                time.sleep(session.config["delay"])

        return reports


class ProcessesNameSpace:
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session, task):  # noqa: N805
        if session.config["n_workers"] > 1:
            bytes_ = cloudpickle.dumps(task)
            return session.executor.submit(unserialize_and_execute_task, bytes_)


def unserialize_and_execute_task(bytes_):
    task = cloudpickle.loads(bytes_)
    task.execute()


class ThreadsNameSpace:
    @hookimpl(tryfirst=True)
    def pytask_execute_task(session, task):  # noqa: N805
        if session.config["n_workers"] > 1:
            return session.executor.submit(task.execute)
