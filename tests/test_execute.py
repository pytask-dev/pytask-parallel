import pickle
import textwrap
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pytest
from pytask.main import main
from pytask_parallel.execute import ProcessesNameSpace
from pytask_parallel.execute import ThreadsNameSpace


class DummyTask:
    def __init__(self, function):
        self.function = function

    def execute(self):
        self.function()


class Session:
    pass


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", ["processes", "threads"])
def test_parallel_execution_speedup(tmp_path, parallel_backend):
    source = """
    import pytask
    import time

    @pytask.mark.produces("out_1.txt")
    def task_1(produces):
        time.sleep(5)
        produces.write_text("1")

    @pytask.mark.produces("out_2.txt")
    def task_2(produces):
        time.sleep(5)
        produces.write_text("2")
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    session = main({"paths": tmp_path})

    assert session.execution_end - session.execution_start > 10

    tmp_path.joinpath("out_1.txt").unlink()
    tmp_path.joinpath("out_2.txt").unlink()

    session = main(
        {"paths": tmp_path, "n_workers": 2, "parallel_backend": parallel_backend}
    )

    assert session.execution_end - session.execution_start < 12


@pytest.mark.integration
def test_pytask_execute_task_w_processes():
    # Local function which cannot be used with multiprocessing.
    def myfunc():
        return 1

    # Verify it cannot be used with multiprocessing because it cannot be pickled.
    with pytest.raises(AttributeError):
        pickle.dumps(myfunc)

    task = DummyTask(myfunc)

    session = Session()
    session.config = {"n_workers": 2, "parallel_backend": "processes"}

    with ProcessPoolExecutor(max_workers=session.config["n_workers"]) as executor:
        session.executor = executor
        future = ProcessesNameSpace.pytask_execute_task(session, task)
        executor.shutdown()

    assert future.result() is None


@pytest.mark.integration
def test_pytask_execute_task_w_threads():
    def myfunc():
        return 1

    task = DummyTask(myfunc)

    session = Session()
    session.config = {"n_workers": 2, "parallel_backend": "threads"}

    with ThreadPoolExecutor(max_workers=session.config["n_workers"]) as executor:
        session.executor = executor
        future = ThreadsNameSpace.pytask_execute_task(session, task)

        executor.shutdown()

    assert future.result() is None
