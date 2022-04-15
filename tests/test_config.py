from __future__ import annotations

import os
import textwrap

import pytest
from pytask import ExitCode
from pytask import main
from pytask_parallel.backends import PARALLEL_BACKENDS


@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "pdb, n_workers, expected",
    [
        (False, 1, 1),
        (True, 1, 1),
        (True, 2, 1),
        (False, 2, 2),
        (False, "auto", os.cpu_count() - 1),
    ],
)
def test_interplay_between_debugging_and_parallel(tmp_path, pdb, n_workers, expected):
    session = main({"paths": tmp_path, "pdb": pdb, "n_workers": n_workers})
    assert session.config["n_workers"] == expected


@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "config_file", ["pytask.ini", "tox.ini", "setup.cfg", "pyproject.toml"]
)
@pytest.mark.parametrize(
    "configuration_option, value, exit_code",
    [
        ("n_workers", "auto", ExitCode.OK),
        ("n_workers", 1, ExitCode.OK),
        ("n_workers", 2, ExitCode.OK),
        ("delay", 0.1, ExitCode.OK),
        ("delay", 1, ExitCode.OK),
        ("parallel_backend", "unknown_backend", ExitCode.CONFIGURATION_FAILED),
    ]
    + [
        ("parallel_backend", parallel_backend, ExitCode.OK)
        for parallel_backend in PARALLEL_BACKENDS
    ],
)
def test_reading_values_from_config_file(
    tmp_path, capsys, config_file, configuration_option, value, exit_code
):
    if config_file == "pyproject.toml":
        config = f"""
        [tool.pytask.ini_options]
        {configuration_option} = {value!r}
        """
    else:
        config = f"""
        [pytask]
        {configuration_option} = {value}
        """
    tmp_path.joinpath(config_file).write_text(textwrap.dedent(config))

    session = main({"paths": tmp_path})
    captured = capsys.readouterr()
    if config_file == "pyproject.toml":
        assert "WARNING" not in captured.out
    else:
        assert "WARNING" in captured.out

    assert session.exit_code == exit_code
    if value == "auto":
        value = os.cpu_count() - 1
    if session.exit_code == ExitCode.OK:
        assert session.config[configuration_option] == value
