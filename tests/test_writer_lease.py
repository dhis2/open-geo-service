"""One writer per instance: the writer lease, taken by the server and by every store write."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from open_geo_service import config as api_config
from open_geo_service import writer_lease
from open_geo_service.features.services import refresh_feature_collection
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.ingestions.processes import execute_ingestion
from open_geo_service.jobs import store as job_store
from open_geo_service.jobs.models import JobRecord, JobStatus
from open_geo_service.jobs.service import reset_job_service
from open_geo_service.main import app
from open_geo_service.openeo.jobs import reset_openeo_job_service
from open_geo_service.pipelines.store import delete_record as delete_pipeline_record
from open_geo_service.shared.time import utc_now
from open_geo_service.tables.store import write_rows
from open_geo_service.writer_lease import WriterLeaseHeld

_HOLD = """
import sys
from open_geo_service import writer_lease
with writer_lease.hold("server"):
    print("held", flush=True)
    sys.stdin.read()
"""


def start_other_writer() -> subprocess.Popen[str]:
    """Another process holding the lease of the configured data folder, until it is killed."""
    writer_lease.release_all()  # this process took it if an earlier step wrote a store
    process = subprocess.Popen([sys.executable, "-c", _HOLD], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    assert process.stdout is not None and process.stdout.readline() == "held\n"
    return process


_FORK_AFTER_TAKING_THE_LEASE = """
import os, sys
from open_geo_service import writer_lease
writer_lease.ensure_held()
child = os.fork()
if child == 0:  # inherits the locked file, but is not the writer
    try:
        writer_lease.ensure_held()
    except writer_lease.WriterLeaseHeld:
        os._exit(0)
    os._exit(1)
sys.exit(0 if os.waitpid(child, 0)[1] == 0 else 1)
"""


@pytest.fixture
def other_writer() -> Generator[subprocess.Popen[str]]:
    process = start_other_writer()
    yield process
    process.kill()
    process.wait()


@pytest.fixture(autouse=True)
def _live_job_services() -> Generator[None]:
    yield
    # The lifespan shuts the job services down; later tests in this process need live ones.
    reset_job_service()
    reset_openeo_job_service()


# The server runs threads; the forked child only checks the lease.
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded, use of fork:DeprecationWarning")
def test_a_second_writer_is_refused_until_the_first_exits(other_writer: subprocess.Popen[str]) -> None:
    with pytest.raises(WriterLeaseHeld, match=rf"server \(pid {other_writer.pid} on .+\) is already writing"):
        with TestClient(app):
            pass

    other_writer.kill()  # a crash: the operating system releases the lock
    other_writer.wait()
    if hasattr(os, "fork"):
        # In a fresh process that forks before starting any thread: forking this one, which runs
        # Icechunk's and obstore's threads, can crash the child before it checks anything.
        forked = subprocess.run([sys.executable, "-c", _FORK_AFTER_TAKING_THE_LEASE], capture_output=True, text=True)
        assert forked.returncode == 0, f"a forked child was let write beside its parent: {forked.stderr}"
    with TestClient(app) as client:
        assert client.get("/info").status_code == 200

    # Kept after shutdown, while job workers stopped without waiting may still write.
    takeover = subprocess.run(
        [sys.executable, "-c", "from open_geo_service import writer_lease; writer_lease.ensure_held()"],
        capture_output=True,
        text=True,
    )
    assert takeover.returncode != 0 and "server (pid" in takeover.stderr


@pytest.fixture
def interrupted_job() -> JobRecord:
    """A job the writer was running, recorded before it starts."""
    return job_store.create_job_record(
        JobRecord(job_id="interrupted", process_id="ingestion", status=JobStatus.RUNNING, created_at=utc_now())
    )


def test_only_the_writer_writes_and_a_read_only_server_runs_beside_it(
    interrupted_job: JobRecord, other_writer: subprocess.Popen[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A script or notebook on the host, writing each kind of data and a record without taking the lease first.
    refused = rf"server \(pid {other_writer.pid} on .+\) is already writing"
    with pytest.raises(WriterLeaseHeld, match=refused):
        execute_ingestion(dataset_id="chirps3_precipitation_daily", start="2026-01-01", end="2026-01-02")
    with pytest.raises(WriterLeaseHeld, match=refused):
        refresh_feature_collection(
            template={"id": "districts", "name": "Districts"}, features={"type": "FeatureCollection", "features": []}
        )
    with pytest.raises(WriterLeaseHeld, match=refused):
        write_rows(dataset_id="precip_districts", collection_id="districts", rows=[], period_type="monthly")
    with pytest.raises(WriterLeaseHeld, match=refused):
        job_store.upsert_job_record(interrupted_job.model_copy(update={"status": JobStatus.FAILED}))
    with pytest.raises(WriterLeaseHeld, match=refused):
        delete_pipeline_record("monthly_export")

    monkeypatch.setattr(api_config, "is_read_only", lambda: True)
    with TestClient(app) as client:
        assert client.get("/info").json()["read_only"] is True
        assert job_store.get_job_record("interrupted") == interrupted_job, "the writer's job is left to the writer"
        with pytest.raises(WriterLeaseHeld, match="This server is read-only"):
            job_store.upsert_job_record(interrupted_job.model_copy(update={"status": JobStatus.FAILED}))

        # Started before the writer has created any index: reads serve none and create none.
        monkeypatch.setattr(ingestion_services, "ARTIFACTS_INDEX_PATH", tmp_path / "records.json")
        monkeypatch.setattr(job_store, "JOBS_INDEX_PATH", tmp_path / "jobs" / "jobs.json")
        monkeypatch.setattr(api_config, "get_data_root", lambda: tmp_path / "data")
        assert client.get("/datasets").status_code == 200
        # The catalog is the writer's to write; until then there is none, as on a static host.
        assert client.get("/stac/catalog.json").status_code == 404
        assert job_store.list_job_records() == []
        assert list(tmp_path.iterdir()) == []


def test_opening_a_store_drops_only_the_concurrent_commits_warning(tmp_path: Path) -> None:
    # A fresh process: Icechunk emits each of these warnings once per process.
    script = (
        "import open_geo_service.startup, icechunk, sys\n"
        "icechunk.Repository.create(icechunk.local_filesystem_storage(sys.argv[1]))\n"
        "icechunk.Repository.open(icechunk.local_filesystem_storage(sys.argv[1]))\n"
    )
    opened = subprocess.run([sys.executable, "-c", script, str(tmp_path)], capture_output=True, text=True, check=True)

    assert "conditional PUT is enabled" in opened.stderr, "Icechunk's other warnings still show"
    assert "concurrent commits" not in opened.stderr
    assert "WARNING" not in opened.stderr, "importing GeoService leaves the root logger to the application"
