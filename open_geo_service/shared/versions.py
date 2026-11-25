"""Removing the replaced versions of a dataset written as one new file per version.

Feature collections and table datasets never overwrite a file: each write is a new file, and the
record says which one is current. A reader resolves the record, then opens the file it names, so
a file just replaced must stay for a reader caught between the two.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path

logger = logging.getLogger(__name__)

SUPERSEDED_FILE_GRACE_SECONDS = 60
"""How long a replaced file stays after the file that replaced it was written.

Far longer than opening one file takes. It is not a guarantee for a reader that pauses for
longer while further writes run; nothing short of reference counting is.
"""


def prune_superseded(
    files: Iterable[Path], *, keep: Iterable[Path], remove: Callable[[Path], None] = os.unlink
) -> None:
    """Remove each of *files* that a newer one replaced more than the grace period ago.

    A file stopped being current when the next newer file was written, so the next file's mtime
    is that moment, and files must become current in the order of their mtimes: a write that is
    rolled back sets the restored file's mtime with :func:`make_current`. *keep* is the file the
    record names and, after a write, the file the record named before it: that file was current
    until this moment, however old the file after it, which may be one a write that never
    registered left behind. Those and the newest file are never removed. A file that cannot be
    removed is logged and tried again on the next write.
    """
    kept = set(keep)
    ordered = sorted(files, key=lambda path: path.stat().st_mtime)
    now = time.time()
    for older, newer in zip(ordered, ordered[1:], strict=False):
        if older in kept or now - newer.stat().st_mtime < SUPERSEDED_FILE_GRACE_SECONDS:
            continue
        try:
            remove(older)
        except OSError:
            logger.warning("Could not remove the superseded file '%s'", older, exc_info=True)


def make_current(path: Path) -> None:
    """Mark *path* as the newest version again, after the write that replaced it was rolled back."""
    os.utime(path)
