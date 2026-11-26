"""Capture and render source excerpts for failures on remote workers."""

from __future__ import annotations

import linecache
import pickle
from dataclasses import dataclass
from dataclasses import field
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING
from typing import TypeAlias

from pytask import ExecutionReport
from pytask import Traceback
from rich.columns import Columns
from rich.console import group
from rich.scope import render_scope
from rich.syntax import Syntax
from rich.text import Text
from rich.traceback import Trace
from rich.traceback import Traceback as RichTraceback

if TYPE_CHECKING:
    from collections.abc import Iterator

    from rich.console import Console
    from rich.console import ConsoleOptions
    from rich.console import RenderResult
    from rich.traceback import Frame
    from rich.traceback import Stack

_CONTEXT_LINES = 3
_MAX_EXCERPT_BYTES = 8 * 1024
_MAX_SOURCE_BYTES = 64 * 1024
_RICH_VERSION = version("rich")

SourceExcerpts: TypeAlias = dict[tuple[str, int], tuple[int, str]]


@dataclass(frozen=True)
class RemoteTraceback:
    """Worker traceback data that can be decoded independently of the result."""

    # Serialize the trace separately so decoding it can fail without losing the
    # worker result and its text fallback.
    trace: bytes
    excerpts: SourceExcerpts
    rich_version: str = _RICH_VERSION

    @classmethod
    def capture(cls, trace: Trace) -> RemoteTraceback | None:
        """Package the trace and bounded excerpts for transfer from a worker."""
        try:
            serialized = pickle.dumps(trace)
        except (AttributeError, pickle.PickleError, TypeError):
            return None

        excerpts: SourceExcerpts = {}
        remaining = _MAX_SOURCE_BYTES
        for frame in _iter_frames(trace):
            key = frame.filename, frame.lineno
            if key in excerpts or frame.filename.startswith("<"):
                continue
            try:
                if not Path(frame.filename).is_file():
                    continue
                linecache.checkcache(frame.filename)
                lines = linecache.getlines(frame.filename)
            except (OSError, UnicodeError, ValueError):
                continue
            if not 1 <= frame.lineno <= len(lines):
                continue
            start = max(1, frame.lineno - _CONTEXT_LINES)
            excerpt = "".join(lines[start - 1 : frame.lineno + _CONTEXT_LINES])
            size = len(excerpt.encode("utf-8", errors="replace"))
            if 0 < size <= min(remaining, _MAX_EXCERPT_BYTES):
                excerpts[key] = start, excerpt
                remaining -= size
                if not remaining:
                    break
        return cls(serialized, excerpts)

    def decode(self) -> Trace | None:
        """Read the trace, falling back to text if the Rich types are incompatible."""
        if self.rich_version != _RICH_VERSION:
            return None
        try:
            trace = pickle.loads(self.trace)  # noqa: S301
        except (AttributeError, EOFError, ImportError, pickle.PickleError, TypeError):
            return None
        return trace if isinstance(trace, Trace) else None


def _iter_frames(trace: Trace) -> Iterator[Frame]:
    """Visit frames nearest each failure first."""
    for stack in trace.stacks:
        yield from reversed(stack.frames)
        for exception in stack.exceptions:
            yield from _iter_frames(exception)


@dataclass
class RemoteExecutionReport(ExecutionReport):
    """An execution report with source lines captured on the worker."""

    excerpts: SourceExcerpts = field(default_factory=dict)

    def __rich_console__(
        self, console: Console, console_options: ConsoleOptions
    ) -> RenderResult:
        """Render worker frames while preserving the rest of the pytask report."""
        for item in super().__rich_console__(console, console_options):
            if isinstance(item, Traceback) and isinstance(item.exc_info[2], Trace):
                yield _RemoteRichTraceback(item.exc_info[2], self.excerpts)
            else:
                yield item


class _RemoteRichTraceback(RichTraceback):
    def __init__(self, trace: Trace, excerpts: SourceExcerpts) -> None:
        super().__init__(trace=trace, width=None)
        self._excerpts = excerpts

    @group()
    def _render_stack(self, stack: Stack) -> RenderResult:
        # Rich owns the exception panels, chains, and groups. Only the frames need
        # a different renderer because their paths and source belong to the worker.
        cutoff = self.max_frames // 2
        hidden = len(stack.frames) - 2 * cutoff if cutoff else 0
        for index, frame in enumerate(stack.frames):
            if hidden > 0 and cutoff <= index < cutoff + hidden:
                if index == cutoff:
                    yield Text(f"... {hidden} frames hidden ...", justify="center")
                continue
            if index:
                yield ""
            yield Text.assemble(
                (frame.filename, "pygments.string"),
                ":",
                (str(frame.lineno), "pygments.number"),
                " in ",
                (frame.name, "pygments.function"),
            )

            panels = []
            excerpt = self._excerpts.get((frame.filename, frame.lineno))
            if excerpt is not None:
                start, source = excerpt
                panels.append(
                    Syntax(
                        source,
                        "python",
                        theme=self.theme,
                        line_numbers=True,
                        start_line=start,
                        highlight_lines={frame.lineno},
                        indent_guides=self.indent_guides,
                    )
                )
            if frame.locals:
                panels.append(render_scope(frame.locals, title="locals"))
            if panels:
                yield Columns(panels, padding=1) if len(panels) > 1 else panels[0]
