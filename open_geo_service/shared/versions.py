"""Removing the replaced versions of a dataset written as one new file per version.

Feature collections and table datasets never overwrite a file: each write is a new file, and the
record says which one is current. A reader resolves the record, then opens the file it names, so
a file just replaced must stay for a reader caught between the two. The record lists each such
file with the time it was replaced (`ArtifactRecord.replaced_files`); no file time is read, as
object storage cannot set one.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from open_geo_service import storage
from open_geo_service.ingestions.schemas import ArtifactRecord, ReplacedFile

logger = logging.getLogger(__name__)

SUPERSEDED_FILE_GRACE_SECONDS = 60
"""How long a replaced file stays listed after it was replaced.

Far longer than opening one file takes. It is not a guarantee for a reader that pauses for
longer while further writes run; nothing short of reference counting is.
"""


def replaced_files(previous: ArtifactRecord | None, replaced: Iterable[Path]) -> list[ReplacedFile]:
    """The files a new record lists as replaced: *replaced*, as of now, and *previous*'s still in grace.

    A file *previous* listed longer ago than the grace period is dropped, and `remove_unneeded`
    removes it once the new record is stored, unless the STAC catalog lags the records: it may
    still link the file, so every listed file is kept until a write after it has caught up.
    """
    from open_geo_service.stac.catalog_files import catalog_behind

    now = datetime.now(UTC)
    settled = now - timedelta(seconds=SUPERSEDED_FILE_GRACE_SECONDS)
    new = [ReplacedFile(path=str(path), replaced_at=now) for path in replaced]
    names = {storage.key(entry.path) for entry in new}
    kept = [
        entry
        for entry in (previous.replaced_files if previous else [])
        if storage.key(entry.path) not in names and (catalog_behind() or entry.replaced_at > settled)
    ]
    return kept + new


def needed_keys(record: ArtifactRecord) -> set[str]:
    """The keys of the files *record* names or lists as replaced; every other file in its folder can go."""
    raw = [record.path, *record.asset_paths, *(entry.path for entry in record.replaced_files)]
    return {storage.key(path) for path in raw if path}


def remove_unneeded(
    files: Iterable[Path], record: ArtifactRecord, *, remove: Callable[[Path], None] | None = None
) -> None:
    """Remove each of *files* that *record* neither names nor lists as replaced.

    Call once *record* is stored, holding the dataset's lock. Such a file was replaced longer ago
    than the grace period, or no record ever named it, as when a write was killed before its
    record. A file that cannot be removed is logged and tried again by the next write.
    """
    removing = remove or storage.remove
    needed = needed_keys(record)
    for path in files:
        if storage.key(path) in needed:
            continue
        try:
            removing(path)
        except OSError:
            logger.warning("Could not remove the superseded file '%s'", path, exc_info=True)
