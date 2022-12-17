"""Extend the build command."""
from __future__ import annotations

import click
from pytask import hookimpl
from pytask_parallel.backends import PARALLEL_BACKENDS_DEFAULT
from pytask_parallel.backends import ParallelBackendsChoices


@hookimpl
def pytask_extend_command_line_interface(cli: click.Group) -> None:
    """Extend the command line interface."""
    additional_parameters = [
        click.Option(
            ["-n", "--n-workers"],
            help=(
                "Max. number of pytask_parallel tasks. Integer >= 1 or 'auto' which is "
                "os.cpu_count() - 1."
            ),
            metavar="[INTEGER|auto]",
            default=1,
        ),
        click.Option(
            ["--parallel-backend"],
            type=click.Choice(ParallelBackendsChoices),
            help="Backend for the parallelization.",
            default=PARALLEL_BACKENDS_DEFAULT,
        ),
    ]
    cli.commands["build"].params.extend(additional_parameters)
