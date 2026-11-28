"""Where GeoService's data lives: the one way into the data folder's `catalog/`.

Every Icechunk repository is opened through :func:`icechunk_storage`, every record, data file and
derived file is written through the object interface (:class:`Objects`), feature collection and
table files are found, measured and served through the functions here, and Parquet is read by
key through :func:`parquet_filesystem`, so a backend is chosen in one place. The local data
folder (`local`) is the only backend so far; object storage comes by configuration (CLIM-1331),
and nothing here may rely on renaming, which object storage does not have. The boundary is
explained in docs/data_folder_layout.md.
"""

from __future__ import annotations

import functools
import os
from pathlib import Path

import icechunk
import pyarrow.fs
from fastapi.responses import FileResponse

from open_geo_service import config as api_config
from open_geo_service.storage.local import Listed, Objects

__all__ = [
    "Listed",
    "Objects",
    "exists",
    "file_response",
    "files",
    "icechunk_storage",
    "key",
    "locate",
    "objects",
    "parquet_filesystem",
    "remove",
    "size",
    "write_file",
    "write_state",
]


def objects() -> Objects:
    """The data folder's objects, staged in its `tmp/` folder (`layout.staging_dir`)."""
    return _objects_at(api_config.get_data_root())


@functools.cache
def _objects_at(root: Path) -> Objects:
    # One per data folder, so a read does not build obstore's store again.
    from open_geo_service.shared import layout

    return Objects(root, staging=layout.staging_dir())


def locate(path: Path | str) -> tuple[Objects, str]:
    """The objects holding *path*, a file in the data folder, and its key there.

    For a caller that still addresses files by path. A path outside the data folder is refused:
    object storage has nothing else.
    """
    store = objects()
    # As written first, so a folder of `catalog/` that is a link to another disk stays in it;
    # then resolved, for a path through a link to the data folder itself.
    written = (Path(os.path.abspath(path)), Path(os.path.abspath(store.root)))
    for candidate, root in (written, (Path(path).resolve(), store.root.resolve())):
        if candidate.is_relative_to(root):
            return store, candidate.relative_to(root).as_posix()
    raise ValueError(f"'{path}' is not in the data folder")


def key(path: Path | str) -> str:
    """The key of *path*, a file in the data folder (`locate`)."""
    return locate(path)[1]


def size(path: Path | str) -> int | None:
    """The bytes stored at *path*, or None when there is no file (`locate`)."""
    store, at = locate(path)
    return store.size(at)


def exists(path: Path | str) -> bool:
    """Whether a file is stored at *path* (`locate`)."""
    return size(path) is not None


def files(folder: Path, suffix: str) -> list[Path]:
    """The files directly in *folder* whose names end in *suffix*, sorted (`locate`)."""
    store, prefix = locate(folder)
    keys, _folders = store.level(prefix)
    return sorted(store.path(found) for found in keys if found.endswith(suffix))


def file_response(path: Path | str, *, media_type: str, filename: str) -> FileResponse:
    """A response serving the file at *path*, answering range requests (`locate`)."""
    store, at = locate(path)
    return FileResponse(store.path(at), media_type=media_type, filename=filename)


def write_state(path: Path, data: bytes) -> None:
    """Write *data* at *path*, a file of the writer's own state such as the records, flushed to disk.

    The writer's state stays on the writer's disk, wherever `catalog/` is (docs/data_folder_layout.md),
    so *path* is any local file rather than a key in the data folder.
    """
    from open_geo_service.shared import layout

    Objects(path.parent, staging=layout.staging_dir()).put(path.name, data)


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
    """The filesystem Parquet files are read from, by their keys (`key`)."""
    return pyarrow.fs.SubTreeFileSystem(str(api_config.get_data_root().resolve()), pyarrow.fs.LocalFileSystem())
