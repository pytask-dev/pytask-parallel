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
