"""Validate command line inputs and configuration values."""


def n_workers_callback(value):
    """Validate the n-workers option."""
    if value == "auto":
        pass
    elif value is None or value == "None":
        value = None
    elif isinstance(value, int) and 1 <= value:
        pass
    elif isinstance(value, str) and value.isnumeric():
        value = int(value)
    else:
        raise ValueError("n_processes can either be an integer >= 1, 'auto' or None.")

    return value


def parallel_backend_callback(value):
    """Validate the input for the parallel backend."""
    if value == "None":
        value = None
    if value not in ["processes", "threads", None]:
        raise ValueError("parallel_backend has to be 'processes' or 'threads'.")
    return value


def delay_callback(value):
    """Validate the delay option."""
    if value is None or value == "None":
        value = None
    else:
        try:
            value = float(value)
        except ValueError:
            pass

        if not (isinstance(value, float) and value > 0):
            raise ValueError("delay has to be a number greater than 0.")

    return value
