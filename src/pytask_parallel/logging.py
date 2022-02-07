"""Contains code relevant to logging."""
from __future__ import annotations

from _pytask.config import hookimpl
from _pytask.console import console


@hookimpl(trylast=True)
def pytask_log_session_header(session):
    """Add a note for how many workers are spawned."""
    n_workers = session.config["n_workers"]
    if n_workers > 1:
        console.print(f"Started {n_workers} workers.")
