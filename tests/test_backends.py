import textwrap

import pytest
from pytask import ExitCode
from pytask import cli


@pytest.mark.end_to_end()
def test_error_requesting_custom_backend_without_registration(runner, tmp_path):
    tmp_path.joinpath("task_example.py").write_text("def task_example(): pass")
    result = runner.invoke(cli, [tmp_path.as_posix(), "--parallel-backend", "custom"])
    assert result.exit_code == ExitCode.FAILED
    assert "No registered parallel backend found" in result.output


@pytest.mark.end_to_end()
def test_error_while_instantiating_custom_backend(runner, tmp_path):
    hook_source = """
    from pytask_parallel import ParallelBackend, registry

    def custom_builder(n_workers):
        raise Exception("ERROR")

    registry.register_parallel_backend(ParallelBackend.CUSTOM, custom_builder)

    def task_example(): pass
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(hook_source))
    result = runner.invoke(cli, [tmp_path.as_posix(), "--parallel-backend", "custom"])
    assert result.exit_code == ExitCode.FAILED
    assert "ERROR" in result.output
    assert "Could not instantiate parallel backend 'custom'." in result.output


@pytest.mark.end_to_end()
def test_register_custom_backend(runner, tmp_path):
    source = """
    from loky import get_reusable_executor
    from pytask_parallel import ParallelBackend
    from pytask_parallel import registry

    def custom_builder(n_workers):
        print("Build custom executor.")
        return get_reusable_executor(max_workers=n_workers)

    registry.register_parallel_backend(
        ParallelBackend.CUSTOM, custom_builder, worker_type="processes", remote=False
    )

    def task_example(): pass
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(cli, [tmp_path.as_posix(), "--parallel-backend", "custom"])
    assert result.exit_code == ExitCode.OK
    assert "Build custom executor." in result.output
    assert "1  Succeeded" in result.output
