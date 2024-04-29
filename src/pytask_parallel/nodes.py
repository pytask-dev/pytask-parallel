"""Contains nodes for pytask-parallel."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from _pytask.node_protocols import PNode
from _pytask.node_protocols import PPathNode
from attrs import define

from pytask_parallel.typing import is_local_path


@define(kw_only=True)
class RemotePathNode(PNode):
    """A class to handle path nodes with local paths in remote environments.

    Tasks may use nodes, following :class:`pytask.PPathNode`, with paths pointing to
    local files. These local files should be automatically available in remote
    environments so that users do not have to care about running their tasks locally or
    remotely.

    The :class:`RemotePathNode` allows to send local files over to remote environments
    and back.

    """

    name: str
    node: PPathNode
    signature: str
    value: Any
    is_product: bool
    remote_path: str = ""
    fd: int = -1

    @classmethod
    def from_path_node(cls, node: PPathNode, *, is_product: bool) -> RemotePathNode:
        """Instantiate class from path node."""
        if not is_local_path(node.path):
            msg = "Path is not a local path and does not need to be fixed"
            raise ValueError(msg)

        value = b"" if is_product else node.path.read_bytes()

        return cls(
            name=node.name,
            node=node,
            signature=node.signature,
            value=value,
            is_product=is_product,
        )

    def state(self) -> str | None:
        """Calculate the state of the node."""
        msg = "RemotePathNode does not implement .state()."
        raise NotImplementedError(msg)

    def load(self, is_product: bool = False) -> Path:  # noqa: ARG002, FBT001, FBT002
        """Load the value."""
        # Create a temporary file to store the value.
        self.fd, self.remote_path = tempfile.mkstemp(suffix=self.node.path.name)
        path = Path(self.remote_path)

        # If the file is a dependency, store the value in the file.
        if not self.is_product:
            path.write_bytes(self.value)

        # Patch path in original node and load the node.
        self.node.path = path
        return self.node.load(is_product=self.is_product)

    def save(self, value: Any) -> None:
        """Save strings or bytes to file."""
        self.value = value
