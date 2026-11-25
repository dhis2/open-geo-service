"""Removing the replaced versions of a dataset written as one new file per version.

Feature collections and table datasets never overwrite a file: each write is a new file, and the
record says which one is current. A reader resolves the record, then opens the file it names, so
a file just replaced must stay for a reader caught between the two.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

from open_geo_service import storage

logger = logging.getLogger(__name__)

SUPERSEDED_FILE_GRACE_SECONDS = 60
"""How long a replaced file stays after the file that replaced it was written.

Far longer than opening one file takes. It is not a guarantee for a reader that pauses for
longer while further writes run; nothing short of reference counting is.
"""


def prune_superseded(
    files: Iterable[Path],
    *,
    current: Path | None,
    replaced: Iterable[Path] = (),
    remove: Callable[[Path], None] | None = None,
) -> None:
    """Call when *current* has just become the file the record names, replacing *replaced*.

    A file stopped being current when the next newer file was written, so the next file's mtime
    is that moment, and each of *files* is removed once that is more than the grace period ago.
    For that, files must become current in the order of their mtimes: *current* is given the time
    now, which also covers a file restored by a rollback, and the files written after *replaced*
    that never became current (a write killed before its record) are removed at once, as no
    record ever named them. *current*, *replaced* and the newest file are never removed. A file
    that cannot be removed is logged and tried again on the next write.
    """
    removing = remove or storage.remove
    if current is not None:
        make_current(current)
    replaced_keys = {path.resolve() for path in replaced}
    kept = replaced_keys | ({current.resolve()} if current is not None else set())
    stamped = sorted(_stamped(files))
    if replaced_times := [mtime for mtime, path in stamped if path.resolve() in replaced_keys]:
        never_current = [path for mtime, path in stamped if mtime > max(replaced_times) and path.resolve() not in kept]
        for path in never_current:
            _remove(path, removing)
        stamped = [(mtime, path) for mtime, path in stamped if path not in never_current]
    now = time.time()
    for (_, older), (written, _) in zip(stamped, stamped[1:], strict=False):
        if older.resolve() not in kept and now - written >= SUPERSEDED_FILE_GRACE_SECONDS:
            _remove(older, removing)


def make_current(path: Path) -> None:
    """Make *path* the newest version, as of now. Logged rather than raised on failure."""
    try:
        os.utime(path)
    except OSError:
        logger.warning("Could not mark '%s' as the current version", path, exc_info=True)


def _stamped(files: Iterable[Path]) -> Iterator[tuple[float, Path]]:
    for path in files:
        try:
            yield path.stat().st_mtime, path
        except OSError:
            continue  # gone since it was listed


def _remove(path: Path, remove: Callable[[Path], None]) -> None:
    try:
        remove(path)
    except OSError:
        logger.warning("Could not remove the superseded file '%s'", path, exc_info=True)
