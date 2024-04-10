import pickle
import textwrap

import pytest
from pytask import ExitCode
from pytask import cli


@pytest.fixture(autouse=True)
def _setup_remote_backend(tmp_path):
    source = """
    from loky import get_reusable_executor
    from pytask_parallel import ParallelBackend
    from pytask_parallel import registry

    def custom_builder(n_workers):
        print("Build custom executor.")
        return get_reusable_executor(max_workers=n_workers)

    registry.register_parallel_backend(
        ParallelBackend.CUSTOM, custom_builder, worker_type="processes", remote=True
    )
    """
    tmp_path.joinpath("config.py").write_text(textwrap.dedent(source))


@pytest.mark.end_to_end()
def test_python_node(runner, tmp_path):
    source = """
    from pathlib import Path
    from typing_extensions import Annotated
    from pytask import PythonNode, Product

    first_part = PythonNode()

    def task_first(node: Annotated[PythonNode, first_part, Product]):
        node.save("Hello ")

    full_text = PythonNode()

    def task_second(
        first_part: Annotated[str, first_part]
    ) -> Annotated[str, full_text]:
        return first_part + "World!"

    def task_third(
        full_text: Annotated[str, full_text]
    ) -> Annotated[str, Path("output.txt")]:
        return full_text
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.OK
    assert "3  Succeeded" in result.output
    assert tmp_path.joinpath("output.txt").read_text() == "Hello World!"


@pytest.mark.end_to_end()
def test_local_path_as_input(runner, tmp_path):
    source = """
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example(path: Path = Path("in.txt")) -> Annotated[str, Path("output.txt")]:
        return path.read_text()
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    tmp_path.joinpath("in.txt").write_text("Hello World!")

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.FAILED
    assert "You cannot use a local path" in result.output


@pytest.mark.end_to_end()
def test_local_path_as_product(runner, tmp_path):
    source = """
    from pytask import Product
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example(path: Annotated[Path, Product] = Path("output.txt")):
        return path.write_text("Hello World!")
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.FAILED
    assert "You cannot use a local path" in result.output


@pytest.mark.end_to_end()
def test_local_path_as_return(runner, tmp_path):
    source = """
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example() -> Annotated[str, Path("output.txt")]:
        return "Hello World!"
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.OK
    assert "1  Succeeded" in result.output
    assert tmp_path.joinpath("output.txt").read_text() == "Hello World!"


@pytest.mark.end_to_end()
def test_pickle_node_with_local_path_as_input(runner, tmp_path):
    source = """
    from pytask import PickleNode
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example(
        text: Annotated[str, PickleNode(path=Path("data.pkl"))]
    ) -> Annotated[str, Path("output.txt")]:
        return text
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    tmp_path.joinpath("data.pkl").write_bytes(pickle.dumps("Hello World!"))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.OK
    assert "1  Succeeded" in result.output
    assert tmp_path.joinpath("output.txt").read_text() == "Hello World!"


@pytest.mark.end_to_end()
def test_pickle_node_with_local_path_as_product(runner, tmp_path):
    source = """
    from pytask import PickleNode, Product
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example(
        node: Annotated[PickleNode, PickleNode(path=Path("data.pkl")), Product]
    ):
        node.save("Hello World!")
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.OK
    assert "1  Succeeded" in result.output
    assert pickle.loads(tmp_path.joinpath("data.pkl").read_bytes()) == "Hello World!"  # noqa: S301


@pytest.mark.end_to_end()
def test_pickle_node_with_local_path_as_return(runner, tmp_path):
    source = """
    from pytask import PickleNode
    from pathlib import Path
    from typing_extensions import Annotated

    def task_example() -> Annotated[str, PickleNode(path=Path("data.pkl"))]:
        return "Hello World!"
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))

    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            "--parallel-backend",
            "custom",
            "--hook-module",
            tmp_path.joinpath("config.py").as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.OK
    assert "1  Succeeded" in result.output
    assert pickle.loads(tmp_path.joinpath("data.pkl").read_bytes()) == "Hello World!"  # noqa: S301
