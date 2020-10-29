import click
from _pytask.config import hookimpl


@hookimpl(trylast=True)
def pytask_log_session_header(session):
    n_workers = session.config["n_workers"]
    if n_workers > 1:
        click.echo(f"Started {n_workers} workers.")
