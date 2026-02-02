from __future__ import annotations

import textwrap
from time import time

import pytest
from pytask import ExitCode
from pytask import build
from pytask import cli

from pytask_parallel import ParallelBackend
from pytask_parallel.execute import _Sleeper
from tests.conftest import restore_sys_path_and_module_after_test_execution
from tests.conftest import skip_if_deadlock

_IMPLEMENTED_BACKENDS = [
    pytest.param(
        ParallelBackend.DASK,
        marks=pytest.mark.skip(
            reason="dask cannot handle dynamically imported modules."
        ),
    ),
    pytest.param(ParallelBackend.LOKY, marks=skip_if_deadlock),
    ParallelBackend.PROCESSES,
    ParallelBackend.THREADS,
]


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_parallel_execution(tmp_path, parallel_backend):
    source = """
    from pytask import Product
    from pathlib import Path
    from typing_extensions import Annotated

    def task_1(path: Annotated[Path, Product] = Path("out_1.txt")):
        path.write_text("1")

    def task_2(path: Annotated[Path, Product] = Path("out_2.txt")):
        path.write_text("2")
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    session = build(paths=tmp_path, n_workers=2, parallel_backend=parallel_backend)
    assert session.exit_code == ExitCode.OK
    assert len(session.tasks) == 2
    assert tmp_path.joinpath("out_1.txt").exists()
    assert tmp_path.joinpath("out_2.txt").exists()


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_parallel_execution_w_cli(runner, tmp_path, parallel_backend):
    source = """
    from pytask import Product
    from pathlib import Path
    from typing_extensions import Annotated

    def task_1(path: Annotated[Path, Product] = Path("out_1.txt")):
        path.write_text("1")

    def task_2(path: Annotated[Path, Product] = Path("out_2.txt")):
        path.write_text("2")
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
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
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("out_1.txt").exists()
    assert tmp_path.joinpath("out_2.txt").exists()


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
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


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
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


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
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


@pytest.mark.parametrize(
    "parallel_backend",
    # Capturing warnings is not thread-safe.
    [ParallelBackend.PROCESSES],
)
def test_collect_warnings_from_parallelized_tasks(runner, tmp_path, parallel_backend):
    source = """
    from pytask import task
    import warnings
    from pathlib import Path

    for i in range(2):

        @task(id=str(i), kwargs={"produces": Path(f"{i}.txt")})
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


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_task_that_return(runner, tmp_path, parallel_backend):
    source = """
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example() -> Annotated[str, Path("file.txt")]:
        return "Hello, Darkness, my old friend."
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert (
        tmp_path.joinpath("file.txt").read_text() == "Hello, Darkness, my old friend."
    )


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
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
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert (
        tmp_path.joinpath("file.txt").read_text() == "Hello, Darkness, my old friend."
    )


@pytest.mark.parametrize("flag", ["--pdb", "--trace", "--dry-run"])
@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_parallel_execution_is_deactivated(runner, tmp_path, flag, parallel_backend):
    tmp_path.joinpath("task_example.py").write_text("def task_example(): pass")
    input_ = "c\n" if flag == "--trace" else None
    result = runner.invoke(
        cli,
        [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend, flag],
        input=input_,
    )
    assert result.exit_code == ExitCode.OK
    assert "Started 2 workers" not in result.output


@pytest.mark.parametrize("code", ["breakpoint()", "import pdb; pdb.set_trace()"])
@pytest.mark.parametrize(
    "parallel_backend",
    [i for i in _IMPLEMENTED_BACKENDS if i != ParallelBackend.THREADS],
)
def test_raise_error_on_breakpoint(runner, tmp_path, code, parallel_backend):
    tmp_path.joinpath("task_example.py").write_text(f"def task_example(): {code}")
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.FAILED
    assert "You cannot use 'breakpoint()'" in result.output


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_task_partialed(runner, tmp_path, parallel_backend):
    source = """
    from pathlib import Path
    from pytask import task
    from functools import partial

    def create_text(text):
        return text

    task_example = task(
        produces=Path("file.txt")
    )(partial(create_text, text="Hello, Darkness, my old friend."))
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("file.txt").exists()


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_execute_tasks_and_pass_values_by_python_node_return(
    runner, tmp_path, parallel_backend
):
    source = """
    from pytask import PythonNode
    from typing_extensions import Annotated
    from pathlib import Path

    node_text = PythonNode(name="text")

    def task_create_text() -> Annotated[int, node_text]:
        return "This is the text."

    def task_create_file(
        text: Annotated[int, node_text]
    ) -> Annotated[str, Path("file.txt")]:
        return text
    """
    tmp_path.joinpath("task_module.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("file.txt").read_text() == "This is the text."


@pytest.mark.parametrize("parallel_backend", _IMPLEMENTED_BACKENDS)
def test_execute_tasks_and_pass_values_by_python_node_product(
    runner, tmp_path, parallel_backend
):
    source = """
    from pytask import PythonNode, Product
    from typing_extensions import Annotated
    from pathlib import Path

    node_text = PythonNode(name="text")

    def task_create_text(node: Annotated[PythonNode, Product] = node_text):
        node.save("This is the text.")

    def task_create_file(
        text: Annotated[int, node_text]
    ) -> Annotated[str, Path("file.txt")]:
        return text
    """
    tmp_path.joinpath("task_module.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
    assert tmp_path.joinpath("file.txt").read_text() == "This is the text."


@pytest.mark.parametrize(
    "parallel_backend",
    [
        ParallelBackend.PROCESSES,
        pytest.param(ParallelBackend.LOKY, marks=skip_if_deadlock),
    ],
)
def test_parallel_execution_with_mark_import(runner, tmp_path, parallel_backend):
    source = """
    from pytask import mark, task

    @task
    def task_assert_math():
        assert 2 + 2 == 4
    """
    tmp_path.joinpath("task_mark.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK


@pytest.mark.parametrize(
    "parallel_backend",
    [
        ParallelBackend.PROCESSES,
        pytest.param(ParallelBackend.LOKY, marks=skip_if_deadlock),
    ],
)
def test_parallel_execution_with_mark_import_in_loop(
    runner, tmp_path, parallel_backend
):
    source = """
    from pytask import mark, task

    for data_name in ("a", "b", "c"):

        @task(id=data_name)
        def task_assert_math_loop():
            assert 2 + 2 == 4
    """
    tmp_path.joinpath("task_mark_loop.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK


@pytest.mark.parametrize(
    "parallel_backend",
    [
        ParallelBackend.PROCESSES,
        pytest.param(ParallelBackend.LOKY, marks=skip_if_deadlock),
    ],
)
def test_parallel_execution_with_closed_file_handle(runner, tmp_path, parallel_backend):
    source = """
    from pathlib import Path
    from pytask import task

    data_path = Path(__file__).parent / "data.txt"
    data_path.write_text("hello", encoding="utf-8")

    with data_path.open(encoding="utf-8") as f:
        content = f.read()

    @task
    def task_assert_math():
        assert content == "hello"
    """
    tmp_path.joinpath("task_file.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli, [tmp_path.as_posix(), "-n", "2", "--parallel-backend", parallel_backend]
    )
    assert result.exit_code == ExitCode.OK
