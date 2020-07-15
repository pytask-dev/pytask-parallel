import os

import pytest
from pytask.main import main


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
