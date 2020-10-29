"""Contains code relevant to logging."""
import click
from _pytask.config import hookimpl


@hookimpl(trylast=True)
def pytask_log_session_header(session):
    """Add a note for how many workers are spawned."""
    n_workers = session.config["n_workers"]
    if n_workers > 1:
        click.echo(f"Started {n_workers} workers.")
