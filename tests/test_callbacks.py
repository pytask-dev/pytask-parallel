import functools
from contextlib import ExitStack as does_not_raise  # noqa: N813

import click
import pytest
from pytask_parallel.callbacks import n_workers_callback
from pytask_parallel.callbacks import n_workers_click_callback


partialed_callback = functools.partial(n_workers_click_callback, ctx=None, name=None)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (0, pytest.raises(click.UsageError)),
        (1, does_not_raise()),
        (2, does_not_raise()),
        ("auto", does_not_raise()),
        ("asdad", pytest.raises(click.UsageError)),
    ],
)
@pytest.mark.parametrize("func", [n_workers_callback, partialed_callback])
def test_validate_n_workers(func, value, expectation):
    with expectation:
        func(value=value)
