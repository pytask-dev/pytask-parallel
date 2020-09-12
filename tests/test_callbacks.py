from contextlib import ExitStack as does_not_raise  # noqa: N813

import pytest
from pytask_parallel.callbacks import delay_callback
from pytask_parallel.callbacks import n_workers_callback
from pytask_parallel.callbacks import parallel_backend_callback


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (0, pytest.raises(ValueError)),
        (1, does_not_raise()),
        (2, does_not_raise()),
        ("auto", does_not_raise()),
        ("asdad", pytest.raises(ValueError)),
        (None, does_not_raise()),
        ("None", does_not_raise()),
        ("1", does_not_raise()),
        ("1.1", pytest.raises(ValueError)),
    ],
)
def test_n_workers_callback(value, expectation):
    with expectation:
        n_workers_callback(value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        ("threads", does_not_raise()),
        ("processes", does_not_raise()),
        (1, pytest.raises(ValueError)),
        ("asdad", pytest.raises(ValueError)),
        (None, does_not_raise()),
        ("None", does_not_raise()),
    ],
)
def test_parallel_backend_callback(value, expectation):
    with expectation:
        parallel_backend_callback(value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expectation",
    [
        (-1, pytest.raises(ValueError)),
        (0.1, does_not_raise()),
        (1, does_not_raise()),
        ("asdad", pytest.raises(ValueError)),
        (None, does_not_raise()),
        ("None", does_not_raise()),
    ],
)
def test_delay_callback(value, expectation):
    with expectation:
        delay_callback(value)
