import textwrap

import pytest
from pytask import ExitCode
from pytask import cli

from pytask_parallel import ParallelBackend
from tests.conftest import skip_if_deadlock


@pytest.mark.parametrize(
    "parallel_backend",
    [
        pytest.param(
            ParallelBackend.DASK,
            marks=pytest.mark.skip(
                reason="dask cannot handle dynamically imported modules."
            ),
        ),
        pytest.param(ParallelBackend.LOKY, marks=skip_if_deadlock),
        ParallelBackend.PROCESSES,
    ],
)
@pytest.mark.parametrize("show_capture", ["no", "stdout", "stderr", "all"])
def test_show_capture(tmp_path, runner, parallel_backend, show_capture):
    source = """
    import sys

    def task_show_capture():
        sys.stdout.write("xxxx")
        sys.stderr.write("zzzz")
        raise Exception
    """
    tmp_path.joinpath("task_show_capture.py").write_text(textwrap.dedent(source))

    cmd_arg = "-s" if show_capture == "s" else f"--show-capture={show_capture}"
    result = runner.invoke(
        cli,
        [
            tmp_path.as_posix(),
            cmd_arg,
            "--parallel-backend",
            parallel_backend,
            "-n",
            "2",
        ],
    )

    assert result.exit_code == ExitCode.FAILED

    if show_capture in ("no", "s"):
        assert "Captured" not in result.output
    elif show_capture == "stdout":
        assert "Captured stdout" in result.output
        assert "xxxx" in result.output
        assert "Captured stderr" not in result.output
        # assert "zzzz" not in result.output
    elif show_capture == "stderr":
        assert "Captured stdout" not in result.output
        # assert "xxxx" not in result.output
        assert "Captured stderr" in result.output
        assert "zzzz" in result.output
    elif show_capture == "all":
        assert "Captured stdout" in result.output
        assert "xxxx" in result.output
        assert "Captured stderr" in result.output
        assert "zzzz" in result.output
    else:  # pragma: no cover
        raise NotImplementedError
