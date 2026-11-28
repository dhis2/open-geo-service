"""Where GeoService's data lives: the one way into the data folder's `catalog/`.

Every Icechunk repository is opened, created or looked for through `storage.icechunk`, every record, data file and
derived file is written through the object interface (:class:`Objects`), feature collection and
table files are found, measured and served through the functions here, and Parquet is read by
key through :func:`parquet_filesystem`, so a backend is chosen in one place. The local data
folder (`local`) is the only backend so far; object storage comes by configuration (CLIM-1331),
and nothing here may rely on renaming, which object storage does not have. The boundary is
explained in docs/data_folder_layout.md.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.fs
from fastapi.responses import FileResponse

from open_geo_service import config as api_config
from open_geo_service.storage.icechunk import (
    icechunk_storage,
    open_or_create_repository,
    open_repository,
    repository_exists,
)
from open_geo_service.storage.local import Listed, Objects

__all__ = [
    "Listed",
    "Objects",
    "exists",
    "file_response",
    "files",
    "folders",
    "icechunk_storage",
    "key",
    "locate",
    "objects",
    "open_or_create_repository",
    "open_repository",
    "parquet_filesystem",
    "read",
    "remove",
    "repository_exists",
    "scan",
    "size",
    "stat",
    "write_file",
    "write_state",
]


def objects() -> Objects:
    """The data folder's objects, staged in its `tmp/` folder (`layout.STAGING`), created by the first write."""
    from open_geo_service.shared import layout

    root = api_config.get_data_root()
    return Objects(root, staging=root / layout.STAGING)


def locate(path: Path | str) -> tuple[Objects, str]:
    """The objects holding *path*, a file in the data folder, and its key there.

    For a caller that still addresses files by path. A path outside the data folder is refused:
    object storage has nothing else.
    """
    store = objects()
    # As written first, so a folder of `catalog/` that is a link to another disk stays in it;
    # then resolved, for a path through a link to the data folder itself.
    for candidate, root in (
        (Path(os.path.abspath(path)), Path(os.path.abspath(store.root))),
        (Path(path).resolve(), store.root.resolve()),
    ):
        if candidate.is_relative_to(root):
            return store, candidate.relative_to(root).as_posix()
    raise ValueError(f"'{path}' is not in the data folder")


def key(path: Path | str) -> str:
    """The key of *path*, a file in the data folder (`locate`)."""
    return locate(path)[1]


def stat(path: Path | str) -> Listed | None:
    """The file stored at *path*, its size and when it was last written, or None when there is none (`locate`)."""
    store, at = locate(path)
    return store.stat(at)


def size(path: Path | str) -> int | None:
    """The bytes stored at *path*, or None when there is no file (`locate`)."""
    found = stat(path)
    return found.size if found is not None else None


def exists(path: Path | str) -> bool:
    """Whether a file is stored at *path* (`locate`)."""
    return stat(path) is not None


def read(path: Path | str) -> bytes:
    """The bytes stored at *path*; `FileNotFoundError` when there are none (`locate`)."""
    store, at = locate(path)
    return store.get(at)


def files(folder: Path, suffix: str) -> list[Path]:
    """The files directly in *folder* whose names end in *suffix*, sorted (`locate`)."""
    store, prefix = locate(folder)
    keys, _folders = store.level(prefix)
    return sorted(store.path(found) for found in keys if found.endswith(suffix))


def folders(folder: Path) -> list[Path]:
    """The folders directly in *folder*, sorted (`locate`)."""
    store, prefix = locate(folder)
    _keys, found = store.level(prefix)
    return sorted(store.path(sub.rstrip("/")) for sub in found)


def scan(folder: Path) -> list[Listed]:
    """Every file under *folder*, at any depth (`locate`)."""
    store, prefix = locate(folder)
    return list(store.scan(prefix))


def file_response(
    path: Path | str, *, media_type: str, filename: str | None = None, headers: dict[str, str] | None = None
) -> FileResponse:
    """A response serving the file at *path*, answering range requests (`locate`)."""
    store, at = locate(path)
    return FileResponse(store.path(at), media_type=media_type, filename=filename, headers=headers)


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


def parquet_filesystem() -> pyarrow.fs.FileSystem:
    """The filesystem Parquet files are read from, by their keys (`key`)."""
    return pyarrow.fs.SubTreeFileSystem(str(api_config.get_data_root().resolve()), pyarrow.fs.LocalFileSystem())
