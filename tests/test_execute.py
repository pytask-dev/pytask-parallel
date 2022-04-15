from __future__ import annotations

import pickle
import textwrap
from pathlib import Path
from time import time

import pytest
from pytask import cli
from pytask import ExitCode
from pytask import main
from pytask import Task
from pytask_parallel.backends import PARALLEL_BACKENDS
from pytask_parallel.execute import DefaultBackendNameSpace
from pytask_parallel.execute import ProcessesNameSpace


class Session:
    pass


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
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

    assert session.exit_code == ExitCode.OK
    assert session.execution_end - session.execution_start > 10

    tmp_path.joinpath("out_1.txt").unlink()
    tmp_path.joinpath("out_2.txt").unlink()

    session = main(
        {"paths": tmp_path, "n_workers": 2, "parallel_backend": parallel_backend}
    )

    assert session.exit_code == ExitCode.OK
    assert session.execution_end - session.execution_start < 10


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_parallel_execution_speedup_w_cli(runner, tmp_path, parallel_backend):
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

    start = time()
    result = runner.invoke(cli, [tmp_path.as_posix()])
    end = time()

    assert result.exit_code == ExitCode.OK
    assert end - start > 10

    tmp_path.joinpath("out_1.txt").unlink()
    tmp_path.joinpath("out_2.txt").unlink()

    start = time()
    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--n-workers",
            "2",
            "--parallel-backend",
            parallel_backend,
        ],
    )
    end = time()

    assert result.exit_code == ExitCode.OK
    assert "Started 2 workers." in result.output
    assert end - start < 10


@pytest.mark.integration
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_pytask_execute_task_w_processes(parallel_backend):
    # Local function which cannot be used with multiprocessing.
    def myfunc():
        return 1

    # Verify it cannot be used with multiprocessing because it cannot be pickled.
    with pytest.raises(AttributeError):
        pickle.dumps(myfunc)

    task = Task(base_name="task_example", path=Path(), function=myfunc)

    session = Session()
    session.config = {
        "n_workers": 2,
        "parallel_backend": parallel_backend,
        "show_locals": False,
    }

    with PARALLEL_BACKENDS[parallel_backend](
        max_workers=session.config["n_workers"]
    ) as executor:
        session.executor = executor

        backend_name_space = {
            "processes": ProcessesNameSpace,
            "threads": DefaultBackendNameSpace,
            "loky": DefaultBackendNameSpace,
        }[parallel_backend]

        future = backend_name_space.pytask_execute_task(session, task)
        executor.shutdown()

    assert future.result() is None


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_parallel_execution_delay(tmp_path, parallel_backend):
    source = """
    import pytask

    @pytask.mark.produces("out_1.txt")
    def task_1(produces):
        produces.write_text("1")

    @pytask.mark.produces("out_2.txt")
    def task_2(produces):
        produces.write_text("2")
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    session = main(
        {
            "paths": tmp_path,
            "delay": 3,
            "n_workers": 2,
            "parallel_backend": parallel_backend,
        }
    )

    assert session.exit_code == ExitCode.OK
    assert 3 < session.execution_end - session.execution_start < 10


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_stop_execution_when_max_failures_is_reached(tmp_path, parallel_backend):
    source = """
    import time
    import pytask

    def task_1(): time.sleep(0.2)
    def task_2(): time.sleep(0.1); raise NotImplementedError

    @pytask.mark.try_last
    def task_3(): ...
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    session = main(
        {
            "paths": tmp_path,
            "n_workers": 2,
            "parallel_backend": parallel_backend,
            "max_failures": 1,
        }
    )

    assert session.exit_code == ExitCode.FAILED
    assert len(session.tasks) == 3
    assert len(session.execution_reports) == 2


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_task_priorities(tmp_path, parallel_backend):
    source = """
    import pytask
    import time

    @pytask.mark.try_first
    def task_0():
        time.sleep(0.1)

    def task_1():
        time.sleep(0.1)

    @pytask.mark.try_last
    def task_2():
        time.sleep(0.1)

    @pytask.mark.try_first
    def task_3():
        time.sleep(0.1)

    def task_4():
        time.sleep(0.1)

    @pytask.mark.try_last
    def task_5():
        time.sleep(0.1)
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    session = main(
        {"paths": tmp_path, "parallel_backend": parallel_backend, "n_workers": 2}
    )

    assert session.exit_code == ExitCode.OK
    first_task_name = session.execution_reports[0].task.name
    assert first_task_name.endswith("task_0") or first_task_name.endswith("task_3")
    last_task_name = session.execution_reports[-1].task.name
    assert last_task_name.endswith("task_2") or last_task_name.endswith("task_5")


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
@pytest.mark.parametrize("show_locals", [True, False])
def test_rendering_of_tracebacks_with_rich(
    runner, tmp_path, parallel_backend, show_locals
):
    source = """
    import pytask

    def task_raising_error():
        a = list(range(5))
        raise Exception
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    args = [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    if show_locals:
        args.append("--show-locals")
    result = runner.invoke(cli, args)

    assert result.exit_code == ExitCode.FAILED
    assert "───── Traceback" in result.output
    assert ("───── locals" in result.output) is show_locals
    assert ("[0, 1, 2, 3, 4]" in result.output) is show_locals


@pytest.mark.end_to_end
@pytest.mark.parametrize("parallel_backend", PARALLEL_BACKENDS)
def test_generators_are_removed_from_depends_on_produces(tmp_path, parallel_backend):
    """Only works with pytask >=0.1.9."""
    source = """
    from pathlib import Path
    import pytask

    @pytask.mark.parametrize("produces", [
        ((x for x in ["out.txt", "out_2.txt"]),),
        ["in.txt"],
    ])
    def task_example(produces):
        produces = {0: produces} if isinstance(produces, Path) else produces
        for p in produces.values():
            p.write_text("hihi")
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    session = main(
        {"paths": tmp_path, "parallel_backend": parallel_backend, "n_workers": 2}
    )

    assert session.exit_code == ExitCode.OK
