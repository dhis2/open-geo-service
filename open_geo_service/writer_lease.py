"""The writer lease: one process per instance writes to its data folder.

The rule and what runs beside the writer are in the instance guide (`docs/instance_guide.md`,
"One writer per instance"). The lease is a file lock, so the operating system releases it
when its process exits and a crashed writer never blocks the next one.

The writing server takes it at startup, and every write to a store or a record checks it
(:func:`ensure_held`), so a script or notebook that writes without taking it first still cannot
write beside the server. Both keep it until the process exits. A host command takes it for its
run (:func:`hold`). A read-only server refuses every write (:func:`refuse_writes`).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import portalocker

from open_geo_service import config as api_config

logger = logging.getLogger(__name__)

LOCK_NAME = "writer.lock"
# Beside the lock rather than in it: on Windows another process cannot read a locked file.
HOLDER_NAME = "writer.json"

# The leases this process holds, by data folder. Several only in tests, which switch instances.
_held: dict[Path, portalocker.Lock] = {}
_held_mutex = threading.Lock()
# Set by a read-only server, which must never become the writer, not even when none is running.
_read_only = False
# One per feature collection or table dataset written in this process (`dataset_lock`).
_dataset_locks: dict[str, threading.Lock] = {}
_dataset_locks_mutex = threading.Lock()


class WriterLeaseHeld(RuntimeError):
    """This process may not write: another process holds the writer lease, or it is a read-only server."""


@contextmanager
def hold(role: str) -> Generator[None]:
    """Hold the writer lease of the configured data folder for the block.

    *role* says what this process is (``"server"``, ``"migrate_layout"``), so a process
    that is refused can name the holder. Raises :class:`WriterLeaseHeld` without waiting
    when another process holds it. If this process already holds it, the block runs under
    that lease and leaves it held.
    """
    data_root = api_config.get_data_root()
    taken = _acquire(data_root, role)
    try:
        yield
    finally:
        if taken:
            _release(data_root)


def ensure_held(role: str = "a script or notebook") -> None:
    """Take the writer lease for the rest of this process, unless it already holds it.

    Called by the writing server at startup and before every write to a store or a record.
    *role* is as for :func:`hold`. Raises :class:`WriterLeaseHeld` when another process holds
    it, or when this process is a read-only server.
    """
    data_root = api_config.get_data_root()
    if _read_only:
        raise WriterLeaseHeld(f"This server is read-only and does not write to {data_root}.")
    if _acquire(data_root, role):
        # A warning for a script or notebook, so it shows under the default logging: while it
        # runs, the server refuses to start.
        level = logging.INFO if role == "server" else logging.WARNING
        logger.log(level, "Took the writer lease of %s for this process, as %s", data_root, role)


def dataset_lock(dataset_id: str) -> threading.Lock:
    """The lock that serializes writes to one feature collection or table dataset, in this process.

    A write puts a new file and then the record naming it, so two writes to one dataset take
    turns, or one could register a file the other's cleanup removes. Within one process is
    enough: the lease keeps every other process from writing. Raises :class:`WriterLeaseHeld`,
    as every write does, when this process may not write.
    """
    ensure_held()
    with _dataset_locks_mutex:
        return _dataset_locks.setdefault(dataset_id, threading.Lock())


def refuse_writes() -> None:
    """Make every later write in this process raise :class:`WriterLeaseHeld`; for a read-only server."""
    global _read_only
    _read_only = True


def release_all() -> None:
    """Release every lease this process holds and allow writes again; for tests.

    The tests run many instances in one process.
    """
    global _read_only
    _read_only = False
    for data_root in list(_held):
        _release(data_root)


def _acquire(data_root: Path, role: str) -> bool:
    """Take the lease of *data_root*; return False when this process already holds it."""
    with _held_mutex:
        if data_root in _held:
            return False
        data_root.mkdir(parents=True, exist_ok=True)
        lock = portalocker.Lock(
            data_root / LOCK_NAME, mode="a", timeout=0, flags=portalocker.LOCK_EX | portalocker.LOCK_NB
        )
        try:
            lock.acquire()
        except portalocker.exceptions.LockException as exc:
            raise WriterLeaseHeld(_refusal(data_root)) from exc
        holder = {
            "role": role,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "since": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        try:
            # Not atomic: a refused process that reads it half-written names "another process".
            (data_root / HOLDER_NAME).write_text(json.dumps(holder), encoding="utf-8")
        except BaseException:
            lock.release()
            raise
        _held[data_root] = lock
        return True


def _release(data_root: Path) -> None:
    with _held_mutex:
        lock = _held.pop(data_root, None)
        if lock is not None:
            (data_root / HOLDER_NAME).unlink(missing_ok=True)
            lock.release()


def _forget_after_fork() -> None:
    """Start a forked child with no lease, so it is refused while its parent holds one.

    The child inherits the parent's locked file and `_held`. Only the child's copy of the file
    is closed: unlocking it would release the lock the two share, and with it the parent's lease.
    """
    global _held_mutex
    _held_mutex = threading.Lock()  # another thread may have held it at the fork
    for lock in _held.values():
        if lock.fh is not None:
            lock.fh.close()
    _held.clear()


if hasattr(os, "register_at_fork"):  # not on Windows, which has no fork
    os.register_at_fork(after_in_child=_forget_after_fork)


def _refusal(data_root: Path) -> str:
    try:
        holder = json.loads((data_root / HOLDER_NAME).read_text(encoding="utf-8"))
        who = f"{holder['role']} (pid {holder['pid']} on {holder['host']}, since {holder['since']})"
    except (OSError, ValueError, KeyError, TypeError):
        who = "another process"
    return (
        f"{who} is already writing to {data_root}. One process per instance writes: stop it first, "
        "or, to run a second server beside it, set read_only: true."
    )
