"""Contains code relevant to logging."""

from __future__ import annotations

from pytask import console
from pytask import hookimpl
from pytask import Session


@hookimpl(trylast=True)
def pytask_log_session_header(session: Session) -> None:
    """Add a note for how many workers are spawned."""
    n_workers = session.config["n_workers"]
    if n_workers > 1:
        console.print(f"Started {n_workers} workers.")
