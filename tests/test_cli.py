from contextlib import ExitStack as does_not_raise  # noqa: N813

import click
import pytest
from pytask_parallel.cli import _validate_n_workers


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (0, pytest.raises(click.UsageError)),
        (1, does_not_raise()),
        (2, does_not_raise()),
        ("auto", does_not_raise()),
    ],
)
def test_validate_n_workers(value, expectation):
    with expectation:
        _validate_n_workers(None, None, value)
