import functools
from contextlib import ExitStack as does_not_raise  # noqa: N813

import click
import pytest
from pytask_parallel.callbacks import delay_callback
from pytask_parallel.callbacks import delay_click_callback
from pytask_parallel.callbacks import n_workers_callback
from pytask_parallel.callbacks import n_workers_click_callback
from pytask_parallel.callbacks import parallel_backend_callback


partialed_n_workers_callback = functools.partial(
    n_workers_click_callback, ctx=None, name=None
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (0, pytest.raises(click.UsageError)),
        (1, does_not_raise()),
        (2, does_not_raise()),
        ("auto", does_not_raise()),
        ("asdad", pytest.raises(click.UsageError)),
        (None, does_not_raise()),
    ],
)
@pytest.mark.parametrize("func", [n_workers_callback, partialed_n_workers_callback])
def test_n_workers_callback(func, value, expectation):
    with expectation:
        func(value=value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        ("threads", does_not_raise()),
        ("processes", does_not_raise()),
        (1, pytest.raises(click.UsageError)),
        ("asdad", pytest.raises(click.UsageError)),
        (None, does_not_raise()),
    ],
)
def test_parallel_backend_callback(value, expectation):
    with expectation:
        parallel_backend_callback(value)


partialed_delay_callback = functools.partial(delay_click_callback, ctx=None, name=None)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (-1, pytest.raises(click.UsageError)),
        (0.1, does_not_raise()),
        (1, does_not_raise()),
        ("asdad", pytest.raises(click.UsageError)),
        (None, does_not_raise()),
    ],
)
@pytest.mark.parametrize("func", [delay_callback, partialed_delay_callback])
def test_delay_callback(func, value, expectation):
    with expectation:
        func(value=value)
