import click
import pytask


@pytask.hookimpl
def pytask_add_parameters_to_cli(command):
    additional_parameters = [
        click.Option(
            ["-n", "--n-processes"],
            help=(
                "Max. number of pytask_parallel tasks. Integer >= 1 or 'auto' which is "
                "os.cpu_count() - 1.  [default: 1 (no parallelization)]"
            ),
            callback=_validate_n_workers,
            metavar="",
        ),
        click.Option(
            ["--pytask_parallel-backend"],
            type=click.Choice(["processes", "threads"]),
            help="Backend for the parallelization.  [default: processes]",
        ),
        click.Option(
            ["--delay"],
            type=float,
            help=(
                "Delay between checking whether tasks have finished.  [default: 0.1 "
                "(seconds)]"
            ),
        ),
    ]
    command.params.extend(additional_parameters)


def _validate_n_workers(ctx, param, value):  # noqa: U100
    if (isinstance(value, int) and value >= 1) or value == "auto":
        pass
    else:
        raise click.UsageError("n-processes can either be an integer >= 1 or 'auto'.")
