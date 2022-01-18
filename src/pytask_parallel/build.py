"""Extend the build command."""
import click
from _pytask.config import hookimpl
from pytask_parallel.backends import PARALLEL_BACKENDS
from pytask_parallel.backends import PARALLEL_BACKENDS_DEFAULT


@hookimpl
def pytask_extend_command_line_interface(cli: click.Group) -> None:
    """Extend the command line interface."""
    additional_parameters = [
        click.Option(
            ["-n", "--n-workers"],
            help=(
                "Max. number of pytask_parallel tasks. Integer >= 1 or 'auto' which is "
                "os.cpu_count() - 1.  [default: 1 (no parallelization)]"
            ),
            metavar="[INTEGER|auto]",
            default=None,
        ),
        click.Option(
            ["--parallel-backend"],
            type=click.Choice(PARALLEL_BACKENDS),
            help=(
                "Backend for the parallelization.  "
                f"[default: {PARALLEL_BACKENDS_DEFAULT}]"
            ),
            default=None,
        ),
        click.Option(
            ["--delay"],
            help=(
                "Delay between checking whether tasks have finished.  [default: 0.1 "
                "(seconds)]"
            ),
            metavar="NUMBER > 0",
            default=None,
        ),
    ]
    cli.commands["build"].params.extend(additional_parameters)
