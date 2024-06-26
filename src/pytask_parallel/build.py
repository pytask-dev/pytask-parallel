"""Extend the build command."""

from __future__ import annotations

import click
from pytask import EnumChoice
from pytask import hookimpl

from pytask_parallel.backends import ParallelBackend


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
            type=EnumChoice(ParallelBackend),
            help="Backend for the parallelization.",
            default=ParallelBackend.NONE,
        ),
    ]
    cli.commands["build"].params.extend(additional_parameters)
