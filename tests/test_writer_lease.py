"""One writer per instance: the writer lease, taken by the server and host commands."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Generator

import icechunk
import pytest
from fastapi.testclient import TestClient

from open_geo_service import config as api_config
from open_geo_service.jobs.service import reset_job_service
from open_geo_service.main import app
from open_geo_service.openeo.jobs import reset_openeo_job_service
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


def test_a_read_only_process_runs_beside_the_writer(
    other_writer: subprocess.Popen[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(api_config, "is_read_only", lambda: True)
    with TestClient(app) as client:
        assert client.get("/info").json()["read_only"] is True


def test_opening_a_store_does_not_warn_about_concurrent_commits(
    tmp_path: pytest.TempPathFactory, capfd: pytest.CaptureFixture[str]
) -> None:
    icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    icechunk.Repository.open(icechunk.local_filesystem_storage(str(tmp_path)))

    assert "not safe for concurrent commits" not in capfd.readouterr().err
