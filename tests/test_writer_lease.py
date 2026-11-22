"""One writer per instance: the writer lease, taken by the server and by every store write."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Generator
from pathlib import Path

import icechunk
import pytest
from fastapi.testclient import TestClient

from open_geo_service import config as api_config
from open_geo_service import writer_lease
from open_geo_service.features.services import refresh_feature_collection
from open_geo_service.ingestions.processes import execute_ingestion
from open_geo_service.jobs.service import reset_job_service
from open_geo_service.main import app
from open_geo_service.openeo.jobs import reset_openeo_job_service
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


def test_a_second_writer_is_refused_until_the_first_exits(other_writer: subprocess.Popen[str]) -> None:
    with pytest.raises(WriterLeaseHeld, match=rf"server \(pid {other_writer.pid} on .+\) is already writing"):
        with TestClient(app):
            pass

    other_writer.kill()  # a crash: the operating system releases the lock
    other_writer.wait()
    with TestClient(app) as client:
        assert client.get("/info").status_code == 200


def test_a_read_only_process_runs_beside_the_writer_but_cannot_write(
    other_writer: subprocess.Popen[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(api_config, "is_read_only", lambda: True)
    with TestClient(app) as client:
        assert client.get("/info").json()["read_only"] is True

    # A script or notebook on the host, writing each kind of data without taking the lease first.
    refused = rf"server \(pid {other_writer.pid} on .+\) is already writing"
    with pytest.raises(WriterLeaseHeld, match=refused):
        execute_ingestion(dataset_id="chirps3_precipitation_daily", start="2026-01-01", end="2026-01-02")
    with pytest.raises(WriterLeaseHeld, match=refused):
        refresh_feature_collection(
            template={"id": "districts", "name": "Districts"}, features={"type": "FeatureCollection", "features": []}
        )
    with pytest.raises(WriterLeaseHeld, match=refused):
        write_rows(dataset_id="precip_districts", collection_id="districts", rows=[], period_type="monthly")


def test_opening_a_store_does_not_warn_about_concurrent_commits(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    icechunk.Repository.open(icechunk.local_filesystem_storage(str(tmp_path)))

    assert "not safe for concurrent commits" not in capfd.readouterr().err
