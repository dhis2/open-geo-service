"""Opening, creating and finding the Icechunk repositories of raster datasets, on whichever backend holds them."""

from __future__ import annotations

from pathlib import Path

import icechunk

from open_geo_service.shared.branches import PUBLISHED_BRANCH, WRITE_BRANCH


def icechunk_storage(store: str | Path) -> icechunk.Storage:
    """The Icechunk storage of the repository at *store*, a path in the data folder or a recorded store path."""
    return icechunk.local_filesystem_storage(str(store))


def repository_exists(store: str | Path) -> bool:
    """Whether an Icechunk repository is at *store*, as Icechunk itself decides it.

    A location Icechunk cannot read as a folder, such as a file, holds none.
    """
    try:
        return bool(icechunk.Repository.exists(icechunk_storage(store)))
    except icechunk.StorageError:
        return False


def open_repository(store: str | Path) -> icechunk.Repository:
    """The Icechunk repository at *store*; Icechunk raises when there is none."""
    return icechunk.Repository.open(icechunk_storage(store))


def open_or_create_repository(store: str | Path) -> icechunk.Repository:
    """The Icechunk repository at *store*, created with its write branch when there is none yet."""
    if repository_exists(store):
        return open_repository(store)
    repo = icechunk.Repository.create(icechunk_storage(store))
    repo.create_branch(WRITE_BRANCH, repo.lookup_branch(PUBLISHED_BRANCH))
    return repo
