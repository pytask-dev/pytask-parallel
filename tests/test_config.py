from __future__ import annotations

import os
import sys
import textwrap

import pytest
from pytask import ExitCode
from pytask import build

from pytask_parallel import ParallelBackend


@pytest.mark.parametrize(
    ("pdb", "n_workers", "expected"),
    [
        (False, 1, 1),
        (True, 1, 1),
        (True, 2, 2),
        (False, 2, 2),
        (False, "auto", (os.cpu_count() or 1) - 1),
    ],
)
def test_interplay_between_debugging_and_parallel(tmp_path, pdb, n_workers, expected):
    session = build(paths=tmp_path, pdb=pdb, n_workers=n_workers)
    assert session.config["n_workers"] == expected


@pytest.mark.parametrize(
    ("configuration_option", "value", "exit_code"),
    [
        ("n_workers", "auto", ExitCode.OK),
        ("n_workers", 1, ExitCode.OK),
        ("n_workers", 2, ExitCode.OK),
        ("parallel_backend", "unknown_backend", ExitCode.CONFIGURATION_FAILED),
        pytest.param(
            "parallel_backend",
            ParallelBackend.LOKY,
            ExitCode.OK,
            marks=pytest.mark.skipif(
                (sys.version_info[:2] == (3, 12) and sys.platform == "win32")
                or (sys.version_info[:2] == (3, 13) and sys.platform == "linux"),
                reason="Deadlock in loky/backend/resource_tracker.py, line 181, maybe related to https://github.com/joblib/loky/pull/450",  # noqa: E501
            ),
        ),
        ("parallel_backend", ParallelBackend.PROCESSES, ExitCode.OK),
        ("parallel_backend", ParallelBackend.THREADS, ExitCode.OK),
        pytest.param(
            "parallel_backend",
            "dask",
            ExitCode.CONFIGURATION_FAILED,
            marks=pytest.mark.skip(reason="Dask is not yet supported"),
        ),
        ("parallel_backend", ParallelBackend.CUSTOM, ExitCode.FAILED),
    ],
)
def test_reading_values_from_config_file(
    tmp_path, configuration_option, value, exit_code
):
    config_value = value.value if isinstance(value, ParallelBackend) else value
    config = f"""
    [tool.pytask.ini_options]
    {configuration_option} = {config_value!r}
    """
    tmp_path.joinpath("pyproject.toml").write_text(textwrap.dedent(config))

    session = build(paths=tmp_path)

    assert session.exit_code == exit_code
    if value == "auto":
        value = (os.cpu_count() or 1) - 1
    if value != "unknown_backend":
        assert session.config[configuration_option] == value
