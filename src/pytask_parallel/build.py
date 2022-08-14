"""Extend the build command."""
from __future__ import annotations

import click
from pytask import hookimpl
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
    ]
    cli.commands["build"].params.extend(additional_parameters)
