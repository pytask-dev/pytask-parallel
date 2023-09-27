from __future__ import annotations

import os
import textwrap

import pytest
from pytask import build
from pytask import ExitCode
from pytask_parallel.backends import ParallelBackend


@pytest.mark.end_to_end()
@pytest.mark.parametrize(
    ("pdb", "n_workers", "expected"),
    [
        (False, 1, 1),
        (True, 1, 1),
        (True, 2, 1),
        (False, 2, 2),
        (False, "auto", os.cpu_count() - 1),
    ],
)
def test_interplay_between_debugging_and_parallel(tmp_path, pdb, n_workers, expected):
    session = build(paths=tmp_path, pdb=pdb, n_workers=n_workers)
    assert session.config["n_workers"] == expected


@pytest.mark.end_to_end()
@pytest.mark.parametrize(
    ("configuration_option", "value", "exit_code"),
    [
        ("n_workers", "auto", ExitCode.OK),
        ("n_workers", 1, ExitCode.OK),
        ("n_workers", 2, ExitCode.OK),
        ("parallel_backend", "unknown_backend", ExitCode.CONFIGURATION_FAILED),
    ]
    + [
        ("parallel_backend", parallel_backend, ExitCode.OK)
        for parallel_backend in ParallelBackend
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
        value = os.cpu_count() - 1
    if session.exit_code == ExitCode.OK:
        assert session.config[configuration_option] == value
