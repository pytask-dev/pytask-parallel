import os
import textwrap

import pytest
from pytask import main


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
@pytest.mark.parametrize("config_file", ["pytask.ini", "tox.ini", "setup.cfg"])
@pytest.mark.parametrize(
    "name, value, exit_code",
    [
        ("n_workers", "auto", 0),
        ("n_workers", 1, 0),
        ("n_workers", 2, 0),
        ("delay", 0.1, 0),
        ("delay", 1, 0),
        ("parallel_backend", "threads", 0),
        ("parallel_backend", "processes", 0),
        ("parallel_backend", "unknown_backend", 2),
    ],
)
def test_reading_values_from_config_file(tmp_path, config_file, name, value, exit_code):
    config = f"""
    [pytask]
    {name} = {value}
    """
    tmp_path.joinpath(config_file).write_text(textwrap.dedent(config))

    session = main({"paths": tmp_path})

    assert session.exit_code == exit_code
    if value == "auto":
        value = os.cpu_count() - 1
    if session.exit_code == 0:
        assert session.config[name] == value
