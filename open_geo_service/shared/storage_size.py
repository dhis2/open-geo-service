"""Measure how much disk a stored artifact takes.

Measured once, when an ingest, sync, openEO publish or feature refresh has finished writing, and
recorded on the artifact as `size_bytes`. Never measured on a page load: a store can be hundreds
of thousands of chunk files, and walking them while a heavy job holds the GIL took the overview
page from seconds to many minutes.
"""

from __future__ import annotations

import logging
from pathlib import Path

from open_geo_service import storage

logger = logging.getLogger(__name__)


def stored_bytes(path: str | Path) -> int:
    """Bytes the artifact at *path* stores.

    For an Icechunk repository, the bytes of the chunks its branches and tags reach, as Icechunk
    reports them: the versions it keeps, without data awaiting collection or its small metadata
    files. For anything else, or when Icechunk cannot tell, the bytes of the files under *path*,
    or of the file at it.

    A path that is missing or unreadable counts as 0 rather than raising, since a size is a
    figure for display and an ingest must not fail over it.
    """
    root = Path(path)
    try:
        if storage.repository_exists(root):
            return int(storage.open_repository(root).chunk_storage_stats().non_virtual_bytes())
    except Exception:
        logger.warning("Could not read the size of '%s' from Icechunk; measuring its files", root, exc_info=True)
    try:
        return storage.size(root) or sum(listed.size for listed in storage.scan(root))
    except (OSError, ValueError):
        return 0
