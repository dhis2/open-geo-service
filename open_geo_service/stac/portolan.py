"""Write a Portolan catalog as files: documents in, a plan of puts and deletes out, applied through a sink.

A stand-in with the shape of portolan-core, the official writer, until it is released
(CLIM-2008), so it imports nothing from GeoService. A document is a STAC dict or Markdown text,
keyed by its path in the catalog (`rasters/chirps/collection.json`). The caller builds the
documents; this decides the order they are written in, which earlier documents to delete, and
writes each file whole.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

DOCUMENT_NAMES = frozenset({"catalog.json", "collection.json", "README.md", "AGENTS.md"})
"""The file names of the documents a catalog is made of; every other file is data."""

Document = Mapping[str, Any] | str


@dataclass(frozen=True)
class Put:
    """Write *body* at *key*, replacing what is there."""

    key: str
    body: bytes


@dataclass(frozen=True)
class Delete:
    """Remove the document at *key*."""

    key: str


Operation = Put | Delete


class Sink(Protocol):
    """Where a plan is applied: a folder, a bucket."""

    def put(self, key: str, body: bytes) -> None: ...

    def delete(self, key: str) -> None: ...


def plan(documents: Mapping[str, Document], prior_managed_keys: Iterable[str]) -> list[Operation]:
    """The operations that make the catalog hold *documents* and none of the other prior documents.

    Deepest first, so a catalog links a child only once the child is written; deletes last, once
    no document links to them any more.
    """
    puts = [Put(key, _encode(document)) for key, document in documents.items()]
    puts.sort(key=lambda put: (-len(PurePosixPath(put.key).parts), put.key))
    deletes = [Delete(key) for key in sorted(set(prior_managed_keys) - set(documents))]
    return [*puts, *deletes]


def apply(operations: Iterable[Operation], sink: Sink) -> None:
    """Carry out *operations* in order."""
    for operation in operations:
        if isinstance(operation, Put):
            sink.put(operation.key, operation.body)
        else:
            sink.delete(operation.key)


def managed_keys(root: Path) -> list[str]:
    """The keys of the documents under *root*: the catalog's, each sub-catalog's and each collection's."""
    if not root.is_dir():
        return []
    found = [path for depth in ("*", "*/*", "*/*/*") for path in root.glob(depth) if path.name in DOCUMENT_NAMES]
    return sorted(path.relative_to(root).as_posix() for path in found if path.is_file())


class FolderSink:
    """A catalog in a local folder.

    Each file is written in *staging*, a folder outside the catalog on the same filesystem, and
    moved into place, so a reader sees the old file or the new one. An unchanged file is left as it is.
    """

    def __init__(self, root: Path, staging: Path) -> None:
        self.root = root
        self.staging = staging

    def put(self, key: str, body: bytes) -> None:
        target = self.root / key
        try:
            if target.read_bytes() == body:
                return
        except FileNotFoundError:
            pass
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(dir=self.staging, prefix=f"{target.name}.", suffix=".tmp")
        try:
            with os.fdopen(descriptor, "wb") as file:
                file.write(body)
            os.chmod(temporary, 0o644)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)


def _encode(document: Document) -> bytes:
    if isinstance(document, str):
        return document.encode("utf-8")
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
