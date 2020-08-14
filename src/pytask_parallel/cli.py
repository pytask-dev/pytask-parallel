import click
from _pytask.config import hookimpl
from pytask_parallel.callbacks import n_workers_click_callback


@hookimpl
def pytask_add_parameters_to_cli(command):
    additional_parameters = [
        click.Option(
            ["-n", "--n-workers"],
            help=(
                "Max. number of pytask_parallel tasks. Integer >= 1 or 'auto' which is "
                "os.cpu_count() - 1.  [default: 1 (no parallelization)]"
            ),
            metavar="[INTEGER|auto]",
            callback=n_workers_click_callback,
        ),
        click.Option(
            ["--parallel-backend"],
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
