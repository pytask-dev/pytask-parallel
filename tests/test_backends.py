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
def test_register_custom_backend(runner, tmp_path):
    source = """
    import cloudpickle

    from concurrent.futures import ProcessPoolExecutor
    from loky import get_reusable_executor
    from pytask_parallel import registry, ParallelBackend

    def _deserialize_and_run_with_cloudpickle(fn: bytes, kwargs: bytes):
        deserialized_fn = cloudpickle.loads(fn)
        deserialized_kwargs = cloudpickle.loads(kwargs)
        return None, [], deserialized_fn(**deserialized_kwargs)

    class _CloudpickleProcessPoolExecutor(ProcessPoolExecutor):

        def submit(self, fn, *args, **kwargs):
            return super().submit(
                _deserialize_and_run_with_cloudpickle,
                fn=cloudpickle.dumps(fn),
                kwargs=cloudpickle.dumps(kwargs),
            )

    def custom_builder(n_workers):
        print("Build custom executor.")
        return _CloudpickleProcessPoolExecutor(max_workers=n_workers)

    registry.register_parallel_backend(ParallelBackend.CUSTOM, custom_builder)

    def task_example(): pass
    """
    tmp_path.joinpath("task_example.py").write_text(textwrap.dedent(source))
    result = runner.invoke(
        cli,
        [tmp_path.as_posix(), "--parallel-backend", "custom"],
    )
    print(result.output)  # noqa: T201
    assert result.exit_code == ExitCode.OK
    assert "Build custom executor." in result.output
    assert "1  Succeeded" in result.output
