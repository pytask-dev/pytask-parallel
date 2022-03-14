from __future__ import annotations

import textwrap
from time import time

import pytest
from pytask import cli
from pytask import ExitCode


@pytest.mark.end_to_end
def test_delay_via_cli(runner, tmp_path):
    source = """
    import pytask

    @pytask.mark.produces("out_1.txt")
    def task_1(produces):
        produces.write_text("1")
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    start = time()
    result = runner.invoke(cli, [tmp_path.as_posix(), "-n", "2", "--delay", "5"])
    end = time()

    assert result.exit_code == ExitCode.OK
    assert end - start > 5
