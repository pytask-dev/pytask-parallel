"""Contains nodes for pytask-parallel."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from _pytask.node_protocols import PNode
from attrs import define

from pytask_parallel.typing import is_local_path

if TYPE_CHECKING:
    from pytask import PathNode


@define(kw_only=True)
class RemotePathNode(PNode):
    """The class for a node which is a path."""

    name: str
    local_path: str
    signature: str
    value: str | bytes
    remote_path: str = ""
    fd: int = -1

    @classmethod
    def from_path_node(cls, node: PathNode, *, is_product: bool) -> RemotePathNode:
        """Instantiate class from path node."""
        if not is_local_path(node.path):
            msg = "Path is not a local path and does not need to be fixed"
            raise ValueError(msg)

        value = b"" if is_product else node.path.read_bytes()

        return cls(
            name=node.name,
            local_path=node.path.as_posix(),
            signature=node.signature,
            value=value,
        )

    def state(self) -> str | None:
        """Calculate the state of the node."""
        msg = "RemotePathNode does not implement .state()."
        raise NotImplementedError(msg)

    def load(self, is_product: bool = False) -> Path:  # noqa: FBT001, FBT002
        """Load the value."""
        # Create a temporary file to store the value.
        ext = os.path.splitext(self.local_path)[1]  # noqa: PTH122
        self.fd, self.remote_path = tempfile.mkstemp(suffix=ext)

        # If the file is a dependency, store the value in the file.
        path = Path(self.remote_path)
        if not is_product:
            path.write_text(self.value) if isinstance(
                self.value, str
            ) else path.write_bytes(self.value)
        return path

    def save(self, value: bytes | str) -> None:
        """Save strings or bytes to file."""
        if isinstance(value, (bytes, str)):
            self.value = value
        else:
            msg = f"'RemotePathNode' can only save 'str' and 'bytes', not {type(value)}"
            raise TypeError(msg)
