"""Contains code relevant to logging."""  # noqa: A005

from __future__ import annotations

from pytask import Session
from pytask import console
from pytask import hookimpl


@hookimpl(trylast=True)
def pytask_log_session_header(session: Session) -> None:
    """Add a note for how many workers are spawned."""
    console.print(f"Starting {session.config['n_workers']} workers.")
