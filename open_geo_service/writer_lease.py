"""The writer lease: one process per instance writes to its data folder.

The rule and what runs beside the writer are in the instance guide (`docs/instance_guide.md`,
"One writer per instance"). The lease is a file lock, so the operating system releases it
when its process exits and a crashed writer never blocks the next one.
"""

from __future__ import annotations

import json
import os
import socket
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import portalocker

from open_geo_service import config as api_config
from open_geo_service.shared.persistence import atomic_text

LOCK_NAME = "writer.lock"
# Beside the lock rather than in it: on Windows another process cannot read a locked file.
HOLDER_NAME = "writer.json"


class WriterLeaseHeld(RuntimeError):
    """Another process holds this instance's writer lease."""


@contextmanager
def hold(role: str) -> Generator[None]:
    """Hold the writer lease of the configured data folder for the block.

    *role* says what this process is (``"server"``, ``"migrate_layout"``), so a process
    that is refused can name the holder. Raises :class:`WriterLeaseHeld` without waiting
    when another process, or another block in this one, holds it.
    """
    data_root = api_config.get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)
    lock = portalocker.Lock(data_root / LOCK_NAME, mode="a", timeout=0, flags=portalocker.LOCK_EX | portalocker.LOCK_NB)
    try:
        lock.acquire()
    except portalocker.exceptions.LockException as exc:
        raise WriterLeaseHeld(_refusal(data_root)) from exc
    holder = data_root / HOLDER_NAME
    try:
        atomic_text(
            holder,
            json.dumps(
                {
                    "role": role,
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "since": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            ),
        )
        yield
    finally:
        holder.unlink(missing_ok=True)
        lock.release()


def _refusal(data_root: Path) -> str:
    try:
        holder = json.loads((data_root / HOLDER_NAME).read_text(encoding="utf-8"))
        who = f"{holder['role']} (pid {holder['pid']} on {holder['host']}, since {holder['since']})"
    except (OSError, ValueError, KeyError, TypeError):
        who = "another process"
    return (
        f"{who} is already writing to {data_root}. One process per instance writes: stop it first, "
        "or set read_only: true to run this one beside it."
    )
