from __future__ import annotations

import textwrap
from time import time

import pytest
from pytask import build
from pytask import cli
from pytask import ExitCode
from pytask_parallel.backends import PARALLEL_BACKENDS
from pytask_parallel.backends import ParallelBackendChoices
from pytask_parallel.execute import _Sleeper

from tests.conftest import restore_sys_path_and_module_after_test_execution


_PARALLEL_BACKENDS_PARAMETRIZATION = [
    pytest.param(i, marks=pytest.mark.xfail(reason="loky fails"))
    if i.value == "loky"
    else pytest.param(i)
    for i in PARALLEL_BACKENDS
]


class Session:
    pass


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
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
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    with restore_sys_path_and_module_after_test_execution():
        session = build(paths=tmp_path)

    assert session.exit_code == ExitCode.OK
    assert session.execution_end - session.execution_start > 10

    tmp_path.joinpath("out_1.txt").unlink()
    tmp_path.joinpath("out_2.txt").unlink()

    session = build(paths=tmp_path, n_workers=2, parallel_backend=parallel_backend)

    assert session.exit_code == ExitCode.OK
    assert session.execution_end - session.execution_start < 10


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
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
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

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


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
def test_stop_execution_when_max_failures_is_reached(tmp_path, parallel_backend):
    source = """
    import time
    import pytask

    def task_1(): time.sleep(1)
    def task_2(): time.sleep(2); raise NotImplementedError

    @pytask.mark.try_last
    def task_3(): time.sleep(3)
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    with restore_sys_path_and_module_after_test_execution():
        session = build(
            paths=tmp_path,
            n_workers=2,
            parallel_backend=parallel_backend,
            max_failures=1,
        )

    assert session.exit_code == ExitCode.FAILED
    assert len(session.tasks) == 3
    assert len(session.execution_reports) == 2


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
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
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    with restore_sys_path_and_module_after_test_execution():
        session = build(paths=tmp_path, parallel_backend=parallel_backend, n_workers=2)

    assert session.exit_code == ExitCode.OK
    first_task_name = session.execution_reports[0].task.name
    assert first_task_name.endswith(("task_0", "task_3"))
    last_task_name = session.execution_reports[-1].task.name
    assert last_task_name.endswith(("task_2", "task_5"))


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
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
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    args = [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    if show_locals:
        args.append("--show-locals")
    result = runner.invoke(cli, args)

    assert result.exit_code == ExitCode.FAILED
    assert "───── Traceback" in result.output
    assert ("───── locals" in result.output) is show_locals
    assert ("[0, 1, 2, 3, 4]" in result.output) is show_locals


@pytest.mark.end_to_end()
@pytest.mark.parametrize(
    "parallel_backend",
    # Capturing warnings is not thread-safe.
    [ParallelBackendChoices.PROCESSES],
)
def test_collect_warnings_from_parallelized_tasks(runner, tmp_path, parallel_backend):
    source = """
    import pytask
    import warnings

    for i in range(2):

        @pytask.mark.task(id=str(i), kwargs={"produces": f"{i}.txt"})
        def task_example(produces):
            warnings.warn("This is a warning.")
            produces.touch()
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )

    assert result.exit_code == ExitCode.OK
    assert "Warnings" in result.output
    assert "This is a warning." in result.output
    assert "capture_warnings.html" in result.output

    warnings_block = result.output.split("Warnings")[1]
    assert "task_example.py::task_example[0]" in warnings_block
    assert "task_example.py::task_example[1]" in warnings_block


def test_sleeper():
    sleeper = _Sleeper(timings=[1, 2, 3], timing_idx=0)

    assert sleeper.timings == [1, 2, 3]
    assert sleeper.timing_idx == 0

    sleeper.increment()
    assert sleeper.timing_idx == 1

    sleeper.increment()
    assert sleeper.timing_idx == 2

    sleeper.reset()
    assert sleeper.timing_idx == 0

    start = time()
    sleeper.sleep()
    end = time()
    assert 1 <= end - start <= 2


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
def test_task_that_return(runner, tmp_path, parallel_backend):
    source = """
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example() -> Annotated[str, Path("file.txt")]:
        return "Hello, Darkness, my old friend."
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("file.txt").exists()


@pytest.mark.end_to_end()
@pytest.mark.parametrize("parallel_backend", _PARALLEL_BACKENDS_PARAMETRIZATION)
def test_task_without_path_that_return(runner, tmp_path, parallel_backend):
    source = """
    from pathlib import Path
    from pytask import task

    task_example = task(
        produces=Path("file.txt")
    )(lambda *x: "Hello, Darkness, my old friend.")
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("file.txt").exists()
