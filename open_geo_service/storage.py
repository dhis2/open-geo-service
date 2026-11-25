"""Where GeoService's data lives: one resolver for Icechunk stores, the object interface, and Parquet reads.

Every Icechunk repository is opened through :func:`icechunk_storage`, every record, data file and
derived file is written through :class:`Objects`, and Parquet is read through
:func:`parquet_filesystem`, so a backend is chosen in one place. The local data folder is the
only backend so far; object storage comes by configuration (CLIM-1331), and nothing here may rely
on renaming, which object storage does not have.

The object interface is obstore. Until a release of obstore can flush a `LocalStore` write to
disk (developmentseed/obstore#822, merged after 0.12.1), a put on the local disk is GeoService's
own flushed write: a temporary file in the data folder's staging folder, flushed, renamed into
place, and the folder flushed.
"""

from __future__ import annotations

import os
import stat
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import icechunk
import obstore
import obstore.exceptions
import pyarrow.fs
from obstore.store import LocalStore

from open_geo_service import config as api_config
from open_geo_service import writer_lease

# Read once at import: os.umask can only be read by setting it, which is not thread-safe later.
_UMASK = os.umask(0)
os.umask(_UMASK)


@dataclass(frozen=True)
class Listed:
    """An object found by a listing: its key, and when it was last written."""

    key: str
    modified: datetime


class Objects:
    """Files under one root, addressed by keys relative to it: `catalog/vectors/a/a.<uuid>.parquet`.

    A put replaces the object whole, so a reader sees the old one or the new one; with
    `create=True` it fails with `FileExistsError` when the key exists. Writes need the writer
    lease, as every write to the data folder does.
    """

    def __init__(self, root: Path, staging: Path | None = None) -> None:
        self.root = root
        self._staging = staging
        self._local: LocalStore | None = None

    @property
    def _store(self) -> LocalStore:
        # Built on first use: obstore needs the root to exist, and a reader may come before any write.
        if self._local is None:
            self._local = LocalStore(prefix=str(self.root), mkdir=True)
        return self._local

    def path(self, key: str) -> Path:
        """The local file behind *key*, for a reader that needs one (a Parquet reader, a file response)."""
        return self.root / key

    def put(self, key: str, data: bytes, *, create: bool = False) -> None:
        """Write *data* at *key*, flushed to disk before returning."""
        writer_lease.ensure_held()
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(dir=self._staging_for(target), prefix=f"{target.name}.", suffix=".tmp")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            self._move_into_place(Path(temporary), target, create=create)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def put_file(self, key: str, source: Path) -> None:
        """Move *source*, a file written in the staging folder, to *key*, flushed to disk before returning.

        For a writer that produces a file rather than bytes: a Parquet writer, a tile builder,
        a plotting library. On object storage this is an upload, then the source is removed.
        """
        writer_lease.ensure_held()
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(source, "rb+") as handle:
            os.fsync(handle.fileno())
        self._move_into_place(source, target, create=False)

    def get(self, key: str) -> bytes:
        """The bytes at *key*; `FileNotFoundError` when there are none."""
        if not self.path(key).is_file():
            raise FileNotFoundError(key)
        return bytes(obstore.get(self._store, key).bytes())

    def delete(self, key: str) -> None:
        """Remove *key*; nothing happens when it is already gone. A failure is an `OSError`, as on a file."""
        writer_lease.ensure_held()
        try:
            obstore.delete(self._store, key)
        except FileNotFoundError:
            pass
        except obstore.exceptions.BaseError as exc:
            raise OSError(f"could not remove '{key}': {exc}") from exc

    def scan(self, prefix: str) -> Iterator[Listed]:
        """Every object under *prefix*, at any depth."""
        if not self.path(prefix).exists():
            return  # obstore would refuse a missing folder; an empty listing is the answer
        for batch in obstore.list(self._store, prefix=prefix):
            for meta in batch:
                yield Listed(meta["path"], meta["last_modified"])

    def level(self, prefix: str) -> tuple[list[str], list[str]]:
        """The keys directly under *prefix*, and the prefixes of its sub-folders, without going deeper."""
        if not self.path(prefix).exists():
            return [], []
        listed = obstore.list_with_delimiter(self._store, prefix=prefix)
        return [meta["path"] for meta in listed["objects"]], list(listed["common_prefixes"])

    def _staging_for(self, target: Path) -> Path:
        # On the target's filesystem, so the rename is atomic, and outside `catalog/`, so a reader
        # of the catalog never meets a partial file.
        staging = self._staging or target.parent
        staging.mkdir(parents=True, exist_ok=True)
        return staging

    def _move_into_place(self, source: Path, target: Path, *, create: bool) -> None:
        os.chmod(source, _file_mode(target))
        if create:
            os.link(source, target)  # fails when the target exists, unlike a rename
            source.unlink()
        else:
            os.replace(source, target)
        if os.name != "nt":
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


def objects() -> Objects:
    """The data folder's objects, staged in its `tmp/` folder (`layout.staging_dir`)."""
    from open_geo_service.shared import layout

    return Objects(api_config.get_data_root(), staging=layout.staging_dir())


def locate(path: Path) -> tuple[Objects, str]:
    """The objects holding *path* and its key there, for a caller that still addresses files by path.

    A path in the data folder is a key in it. One outside it, such as a test's own folder, is
    held by its parent folder.
    """
    store = objects()
    try:
        return store, path.resolve().relative_to(store.root.resolve()).as_posix()
    except ValueError:
        return Objects(path.parent), path.name


def write(path: Path, data: bytes) -> None:
    """Write *data* at *path* through the object interface (`locate`)."""
    store, key = locate(path)
    store.put(key, data)


def write_file(path: Path, source: Path) -> None:
    """Move the staged file *source* to *path* through the object interface (`locate`)."""
    store, key = locate(path)
    store.put_file(key, source)


def remove(path: Path) -> None:
    """Remove *path* through the object interface (`locate`); nothing happens when it is gone."""
    store, key = locate(path)
    store.delete(key)


def icechunk_storage(store: str | Path) -> icechunk.Storage:
    """The Icechunk storage of the repository at *store*, a path in the data folder or a recorded store path."""
    return icechunk.local_filesystem_storage(str(store))


def parquet_filesystem() -> pyarrow.fs.FileSystem:
    """The filesystem Parquet files are read from, with the same paths the records hold."""
    return pyarrow.fs.LocalFileSystem()


def _file_mode(path: Path) -> int:
    """The mode *path* has, or the one a new file gets; a temporary file starts owner-only."""
    try:
        return stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return 0o666 & ~_UMASK
