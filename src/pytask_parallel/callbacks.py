import click


def n_workers_click_callback(ctx, name, value):  # noqa: U100
    return n_workers_callback(value)


def n_workers_callback(value):
    error_occurred = False
    if value == "auto":
        pass
    elif value is None:
        pass
    elif isinstance(value, int) and 1 <= value:
        pass
    else:
        try:
            value = int(value)
        except ValueError:
            error_occurred = True
        else:
            if value < 1:
                error_occurred = True

    if error_occurred:
        raise click.UsageError("n-processes can either be an integer >= 1 or 'auto'.")

    return value


def parallel_backend_callback(value):
    if value not in ["processes", "threads"]:
        raise click.UsageError("parallel_backend has to be 'processes' or 'threads'.")
    return value


def delay_click_callback(ctx, name, value):  # noqa: U100
    return delay_callback(value)


def delay_callback(value):
    if isinstance(value, float) and 0 < value:
        pass
    elif value is None:
        pass
    else:
        try:
            value = float(value)
        except ValueError:
            error_occurred = True
        else:
            if value < 0:
                error_occurred = True

    if error_occurred:
        raise click.UsageError("delay has to be a number greater than 0.")

    return value
