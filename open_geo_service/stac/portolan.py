"""Write a Portolan catalog as files: documents in, a plan of puts and deletes out, applied through a sink.

A stand-in with the shape of portolan-core, the official writer, until it is released
(CLIM-2008), so it imports nothing from GeoService. A document is a STAC dict or Markdown text,
keyed by its path in the catalog (`rasters/chirps/collection.json`). The caller builds the
documents; this decides the order they are written in and which earlier documents to delete,
and the caller's sink writes each file whole.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
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


def managed_keys(keys: Iterable[str]) -> list[str]:
    """Of *keys*, those of the catalog's documents: its own, each sub-catalog's and each collection's."""
    return sorted(
        key for key in keys if PurePosixPath(key).name in DOCUMENT_NAMES and len(PurePosixPath(key).parts) <= 3
    )


def _encode(document: Document) -> bytes:
    if isinstance(document, str):
        return document.encode("utf-8")
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
