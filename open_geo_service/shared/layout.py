"""Where each file in the data folder lives.

Dataset files sit under the catalog root, by kind; GeoService's own state (records, the writer
lease, jobs, staging files, markers) sits beside it at the data folder root. Every path to a
dataset file is built here, as a key relative to the catalog root, and :func:`local` joins a key
to the data folder. A storage backend for object storage replaces that one function (CLIM-555).
"""

from __future__ import annotations

import errno
import os
import shutil
from collections.abc import Iterator
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

from open_geo_service import config as api_config


class Kind(StrEnum):
    """The kinds of dataset, each a folder of its own under the catalog root."""

    RASTER = "rasters"
    VECTOR = "vectors"
    TABLE = "tables"


def kind_of(stored_as: str) -> Kind:
    """The kind of a dataset, from its `ArtifactFormat` or its `DatasetItemType`."""
    if stored_as in ("geoparquet", "feature"):
        return Kind.VECTOR
    if stored_as in ("parquet", "table"):
        return Kind.TABLE
    return Kind.RASTER


def local(key: str) -> Path:
    """The file or folder at *key*, a path relative to the catalog root, in the data folder."""
    return api_config.get_data_root() / key


def kind_root(kind: Kind) -> Path:
    """The folder holding every dataset of *kind*."""
    return local(kind.value)


def dataset_key(kind: Kind, dataset_id: str) -> str:
    """The key of the folder holding *dataset_id*'s files."""
    # Rasters and vectors share one folder per kind; a table has a folder of its own.
    return f"{kind.value}/{dataset_id}" if kind is Kind.TABLE else kind.value


def dataset_dir(kind: Kind, dataset_id: str) -> Path:
    """The folder holding *dataset_id*'s files."""
    return local(dataset_key(kind, dataset_id))


def raster_store(dataset_id: str) -> Path:
    """The Icechunk store of the raster dataset *dataset_id*, whether or not it exists yet."""
    return dataset_dir(Kind.RASTER, dataset_id) / f"{dataset_id}.icechunk"


def raster_zarr(dataset_id: str) -> Path:
    """The plain Zarr store of the raster dataset *dataset_id*, whether or not it exists."""
    return dataset_dir(Kind.RASTER, dataset_id) / f"{dataset_id}.zarr"


def raster_stores() -> Iterator[Path]:
    """Every raster dataset's Icechunk store in the data folder."""
    root = kind_root(Kind.RASTER)
    if root.is_dir():
        yield from sorted(root.glob("*.icechunk"))


def thumbnail(kind: Kind, dataset_id: str) -> Path:
    """The thumbnail of *dataset_id*, whether or not it exists yet."""
    return local(f"thumbnails/{dataset_id}.png")


def find_thumbnail(stored_as: str, dataset_id: str) -> Path | None:
    """The thumbnail of *dataset_id*, or None when it has none.

    *stored_as* is the dataset's `ArtifactFormat` or `DatasetItemType`, as for :func:`kind_of`.
    """
    path = thumbnail(kind_of(stored_as), dataset_id)
    return path if path.is_file() else None


def staging_dir() -> Path:
    """A folder for files being written, which are then moved into place with :func:`move_into_place`.

    Outside the dataset folders, so a reader of them never meets a partial file.
    """
    folder = api_config.get_data_root() / "tmp"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def move_into_place(source: Path, target: Path) -> None:
    """Move *source*, written in :func:`staging_dir`, over *target*, so a reader sees the old file or the new one.

    When *target* is on another filesystem, as when a folder of the data folder is a mount of
    its own, *source* is copied beside *target* under a hidden name first, so the last step is
    still a rename within one filesystem.
    """
    try:
        os.replace(source, target)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    beside = target.with_name(f".{target.name}.{uuid4().hex}.moving")
    try:
        shutil.copyfile(source, beside)
        os.replace(beside, target)
    finally:
        beside.unlink(missing_ok=True)
    source.unlink(missing_ok=True)


def collection_marker(dataset_id: str) -> Path:
    """The marker saying that the raster store of *dataset_id* has data due for collection."""
    return api_config.get_data_root() / "gc-pending" / dataset_id


def pending_collections() -> Iterator[str]:
    """The ids of the raster datasets whose store has a collection marker."""
    folder = api_config.get_data_root() / "gc-pending"
    if folder.is_dir():
        yield from sorted(marker.name for marker in folder.iterdir() if marker.is_file())
