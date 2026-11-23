"""Concurrent writers and interrupted replacement must retain committed job state."""

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from open_geo_service.jobs import store
from open_geo_service.jobs.models import JobRecord, JobStatus
from open_geo_service.shared.time import utc_now


def _increment(start: threading.Barrier, errors: list[BaseException]):
    try:
        start.wait(10)
        for _ in range(10):

            def mutate(record: JobRecord):
                time.sleep(0.002)
                return record.model_copy(update={"request": {"count": record.request["count"] + 1}})

            store.mutate_job_record("counter", mutate)
    except BaseException as error:
        errors.append(error)


def test_writers_do_not_lose_updates_across_atomic_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Threads of the one writing process: a second process is refused by the writer lease.
    monkeypatch.setattr(store, "JOBS_DIR", tmp_path)
    monkeypatch.setattr(store, "JOBS_INDEX_PATH", tmp_path / "jobs.json")
    store.create_job_record(
        JobRecord(
            job_id="counter",
            process_id="test",
            status=JobStatus.ACCEPTED,
            created_at=utc_now(),
            request={"count": 0},
        )
    )
    start = threading.Barrier(4)
    errors: list[BaseException] = []
    # Daemon threads, so one that deadlocks fails the test rather than keeping pytest alive.
    workers = [threading.Thread(target=_increment, args=(start, errors), daemon=True) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(20)
    assert not any(worker.is_alive() for worker in workers), "a writer hung"
    assert errors == []
    record = store.get_job_record("counter")
    assert record is not None and record.request["count"] == 40


def test_failed_replace_preserves_reservations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from open_geo_service import config
    from open_geo_service.exports.reservations import find_delivery, reserve_delivery, submission_lock
    from open_geo_service.shared import persistence

    monkeypatch.setattr(config, "get_data_root", lambda: tmp_path)
    arguments: dict[str, Any] = dict(
        delivery_job_id="job", export_id="rain", source_job_id="source", dry_run=False, fingerprint="a"
    )
    with submission_lock():
        reserve_delivery("first", **arguments)

    def fail(*args: Any):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(persistence.os, "replace", fail)
    with submission_lock(), pytest.raises(OSError):
        reserve_delivery("second", **arguments)
    assert find_delivery("first") is not None
    assert find_delivery("second") is None
