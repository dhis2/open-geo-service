"""Moving an instance from the rasters/vectors folders into catalog/ (CLIM-1968)."""

from __future__ import annotations

import json
import os
import shutil
import stat
from collections.abc import Callable
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from open_geo_service import config as api_config
from open_geo_service import migrate_layout
from open_geo_service.shared import layout
from open_geo_service.stac import portolan

from .test_writer_lease import start_other_writer

UUID = "0123456789abcdef0123456789abcdef"
RASTER = {"dataset_id": "chirps", "format": "icechunk"}
VECTOR = {"dataset_id": "districts", "format": "geoparquet"}


@pytest.fixture
def instance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An instance on the layout before catalog/: a raster store and a feature file, recorded four ways."""
    data = tmp_path / "data"
    (data / "rasters" / "chirps.icechunk").mkdir(parents=True)
    (data / "rasters" / "chirps.icechunk.lock").write_text("")
    (data / "vectors").mkdir()
    (data / "vectors" / f"districts.{UUID}.parquet").write_bytes(b"PAR1")
    (data / "artifacts").mkdir()
    records = [
        {**RASTER, "artifact_id": "a", "path": "rasters/chirps.icechunk", "asset_paths": ["rasters/chirps.icechunk"]},
        {**VECTOR, "artifact_id": "b", "path": str(data / "vectors" / f"districts.{UUID}.parquet"), "asset_paths": []},
        {**RASTER, "artifact_id": "c", "path": "/mnt/elsewhere/rasters/era5.icechunk", "asset_paths": []},
        {**RASTER, "artifact_id": "d", "path": "/app/data/rasters/chirps.icechunk", "asset_paths": []},
    ]
    (data / "artifacts" / "records.json").write_text(json.dumps(records, indent=2) + "\n")

    config = tmp_path / "geoservice.yaml"
    config.write_text(f"data_dir: {data}\n")
    monkeypatch.setenv("GEOSERVICE_CONFIG", str(config))
    api_config._cache = None
    return tmp_path


def test_moves_every_dataset_into_the_catalog_and_rewrites_what_names_them(instance: Path) -> None:
    assert migrate_layout.main([]) == 0

    data = instance / "data"
    assert sorted(p.name for p in data.iterdir()) == ["artifacts", "catalog", "tmp", "writer.lock"]
    assert sorted(p.relative_to(data / "catalog").as_posix() for p in (data / "catalog").glob("*/*/*")) == [
        "rasters/chirps/chirps.icechunk",
        f"vectors/districts/districts.{UUID}.parquet",
    ], "the empty store lock is deleted"

    records = json.loads((data / "artifacts" / "records.json").read_text())
    chirps = "catalog/rasters/chirps/chirps.icechunk"
    assert records[0]["path"] == chirps
    assert records[0]["asset_paths"] == [chirps]
    assert records[1]["path"] == f"catalog/vectors/districts/districts.{UUID}.parquet", (
        "an absolute path under the data dir becomes portable"
    )
    assert records[2]["path"] == "/mnt/elsewhere/rasters/era5.icechunk", "a store outside the data dir is left alone"
    assert records[3]["path"] == chirps, "a store recorded under a container's mount is ours"
    assert layout.raster_store("chirps").is_dir(), "the service finds the store where it now is"


def test_running_it_again_changes_nothing(instance: Path, capsys: pytest.CaptureFixture[str]) -> None:
    migrate_layout.main([])
    records = (instance / "data" / "artifacts" / "records.json").read_text()
    capsys.readouterr()

    assert migrate_layout.main([]) == 0
    assert "Nothing to migrate" in capsys.readouterr().out
    assert (instance / "data" / "artifacts" / "records.json").read_text() == records

    # An old folder holding only macOS folder metadata is removed, so the writer can start.
    (instance / "data" / "rasters").mkdir()
    (instance / "data" / "rasters" / ".DS_Store").write_bytes(b"")
    assert migrate_layout.pending_migration() is not None
    assert migrate_layout.main([]) == 0
    assert not (instance / "data" / "rasters").exists()
    assert migrate_layout.pending_migration() is None

    # A mistyped data_dir is reported as having nothing to migrate, and not created.
    (instance / "geoservice.yaml").write_text(f"data_dir: {instance / 'data-typo'}\n")
    api_config._cache = None
    assert migrate_layout.main([]) == 0
    assert not (instance / "data-typo").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_rewriting_the_records_keeps_their_mode(instance: Path) -> None:
    """The records are replaced through an owner-only temporary file; the migration must not leave them owner-only."""
    index = instance / "data" / "artifacts" / "records.json"
    index.chmod(0o644)

    assert migrate_layout.main([]) == 0
    assert stat.S_IMODE(index.stat().st_mode) == 0o644


def test_a_dry_run_reports_and_changes_nothing(instance: Path, capsys: pytest.CaptureFixture[str]) -> None:
    before = sorted(str(p.relative_to(instance)) for p in instance.rglob("*"))

    assert migrate_layout.main(["--dry-run"]) == 0

    out = capsys.readouterr().out
    assert "would move rasters/chirps.icechunk" in out and "would rewrite store paths" in out
    assert sorted(str(p.relative_to(instance)) for p in instance.rglob("*")) == before


def test_refuses_when_a_dataset_is_already_in_its_new_place(instance: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (instance / "data" / "catalog" / "rasters" / "chirps" / "chirps.icechunk").mkdir(parents=True)

    assert migrate_layout.main([]) == 1

    assert "move their contents by hand" in capsys.readouterr().err
    assert (instance / "data" / "rasters" / "chirps.icechunk").is_dir(), "nothing was moved"


@pytest.mark.parametrize("argv", [["--dry-run"], []], ids=["dry run", "run"])
def test_refuses_to_move_onto_another_filesystem(
    instance: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], argv: list[str]
) -> None:
    """A rename across filesystems fails, which a dry run must report before a run changes anything."""
    index = instance / "data" / "artifacts" / "records.json"
    records = index.read_text()
    lstat = Path.lstat

    def on_another_disk(path: Path) -> os.stat_result:
        found = lstat(path)
        return os.stat_result([*found[:2], found.st_dev + 1, *found[3:10]]) if path.name == "chirps.icechunk" else found

    monkeypatch.setattr(Path, "lstat", on_another_disk)

    assert migrate_layout.main(argv) == 1

    assert "cross filesystems" in capsys.readouterr().err
    assert (instance / "data" / "rasters" / "chirps.icechunk").is_dir(), "nothing was moved"
    assert index.read_text() == records


@pytest.mark.parametrize("earlier", ["data/downloads", "plugins/datasets"])
def test_refuses_on_the_layout_before_rasters_and_vectors(
    instance: Path, capsys: pytest.CaptureFixture[str], earlier: str
) -> None:
    (instance / earlier).mkdir(parents=True)
    (instance / "geoservice.yaml").write_text(f"data_dir: {instance / 'data'}\nplugins_dir: plugins\n")
    api_config._cache = None

    assert migrate_layout.main(["--dry-run"]) == 1

    assert "release from before November 2026 first" in capsys.readouterr().err
    assert (instance / "data" / "rasters" / "chirps.icechunk").is_dir(), "nothing was moved"


def test_refuses_while_a_writer_runs(instance: Path, capsys: pytest.CaptureFixture[str]) -> None:
    writer = start_other_writer()
    try:
        assert migrate_layout.main([]) == 1
    finally:
        writer.kill()
        writer.wait()

    assert f"server (pid {writer.pid}" in capsys.readouterr().err
    assert (instance / "data" / "rasters" / "chirps.icechunk").is_dir(), "nothing was moved"


def test_a_data_dir_reached_through_a_symlink_is_migrated(instance: Path) -> None:
    link = instance / "link"
    link.symlink_to(instance / "data")
    index = instance / "data" / "artifacts" / "records.json"
    index.write_text(json.dumps([{**RASTER, "artifact_id": "a", "path": str(link / "rasters" / "chirps.icechunk")}]))
    (instance / "geoservice.yaml").write_text(f"data_dir: {link}\n")
    api_config._cache = None

    assert migrate_layout.main([]) == 0

    [record] = json.loads(index.read_text())
    assert record["path"] == "catalog/rasters/chirps/chirps.icechunk"


def test_an_external_store_that_still_exists_is_left_alone(instance: Path) -> None:
    """Same suffix as a store under the data dir, but it exists where it was recorded."""
    external = instance / "archive" / "rasters" / "chirps.icechunk"
    external.mkdir(parents=True)
    index = instance / "data" / "artifacts" / "records.json"
    index.write_text(json.dumps([{**RASTER, "artifact_id": "a", "path": str(external)}]))

    assert migrate_layout.main([]) == 0

    [record] = json.loads(index.read_text())
    assert record["path"] == str(external)


# --- a data folder written through the real paths ----------------------------------------------


def _back_to_the_old_layout(data: Path) -> None:
    """Rearrange a data folder written by this version into the layout before catalog/."""
    catalog = data / "catalog"
    (data / "thumbnails").mkdir()
    for kind in ("rasters", "vectors"):
        (data / kind).mkdir()
        for folder in (catalog / kind).iterdir():
            (folder / "thumbnail.png").rename(data / "thumbnails" / f"{folder.name}.png")
            for path in folder.iterdir():
                path.rename(data / kind / path.name)
    (catalog / "tables").rename(data / "tables")
    shutil.rmtree(catalog)
    # What the old layout kept beside the data: an empty store lock, a collection marker, and a
    # marker for a feature file replaced under the old rule.
    (data / "rasters" / "rain.icechunk.lock").write_text("")
    (data / "rasters" / "rain.icechunk.gc-pending").write_text("2000-01-01T00:00:00+00:00")
    (data / "rasters" / "gone.icechunk.gc-pending").write_text("2000-01-01T00:00:00+00:00")
    (data / "vectors" / f"districts.{'0' * 32}.parquet.superseded").write_text("")
    # And files an earlier version was killed writing, inside a table's folder too.
    (data / "vectors" / f"districts.{'1' * 32}.parquet.{'2' * 32}.writing").write_text("")
    (data / "tables" / "rain_districts" / f"x.parquet.{'3' * 32}.writing").write_text("")
    old = {"catalog/rasters/rain/": "rasters/", "catalog/vectors/districts/": "vectors/", "catalog/tables/": "tables/"}
    for index in (data / "artifacts" / "records.json", data / "openeo_jobs" / "jobs.json"):
        text = index.read_text()
        for new, before in old.items():
            text = text.replace(new, before)
        index.write_text(text)


@pytest.fixture
def old_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A data folder before catalog/: a raster, a feature collection and a table, with thumbnails
    and an openEO job that published the raster, written by this version and moved back."""
    from open_geo_service.ingestions import services as ingestion_services

    from .test_catalog_files import write_one_of_each

    data = tmp_path / "data"
    write_one_of_each(data, monkeypatch)
    [record] = [record for record in ingestion_services._load_records() if record.dataset_id == "rain"]
    # The catalog's documents came with that layout; the files of the layout before it did not.
    for path in (data / "catalog").rglob("*"):
        if path.name in portolan.DOCUMENT_NAMES and path.is_file():
            path.unlink()
    for kind, dataset_id in ((layout.Kind.RASTER, "rain"), (layout.Kind.VECTOR, "districts")):
        layout.thumbnail(kind, dataset_id).write_bytes(b"\x89PNG\r\n\x1a\n" + dataset_id.encode())
    jobs = [
        {
            "id": "job-1",
            "status": "finished",
            "usage": {"output_path": "managed://rain"},
            "publication": {"store_path": str(record.path), "record": record.model_dump(mode="json")},
        }
    ]
    (data / "openeo_jobs").mkdir()
    (data / "openeo_jobs" / "jobs.json").write_text(json.dumps(jobs))

    _back_to_the_old_layout(data)
    return data


def test_an_old_data_folder_is_served_from_the_catalog_after_the_migration(
    old_data: Path, client: TestClient, restart_writer: Callable[[], None]
) -> None:
    # A writer refuses to start before the migration, so no sync writes a second store into catalog/.
    with pytest.raises(RuntimeError, match="migrate_layout"):
        restart_writer()
    # As a run stopped after moving the store and before its pending-collection marker left it.
    moved = old_data / "catalog" / "rasters" / "rain" / "rain.icechunk"
    moved.parent.mkdir(parents=True)
    (old_data / "rasters" / "rain.icechunk").rename(moved)
    # The thumbnail of a removed dataset, which nothing serves, goes with the old folder.
    (old_data / "thumbnails" / "removed.png").write_bytes(b"\x89PNG\r\n\x1a\n")

    assert migrate_layout.main([]) == 0

    assert sorted(p.name for p in old_data.iterdir()) == [
        "artifacts",
        "catalog",
        "gc-pending",
        "openeo_jobs",
        "tmp",
    ] + [
        "writer.json",
        "writer.lock",
    ]
    catalog = sorted(p.relative_to(old_data / "catalog").as_posix() for p in (old_data / "catalog").rglob("*"))
    assert [name for name in catalog if name.count("/") == 2 and not name.endswith(".parquet")] == [
        "rasters/rain/rain.icechunk",
        "rasters/rain/thumbnail.png",
        "vectors/districts/thumbnail.png",
    ], "nothing but the data and its thumbnail: no lock, marker or staging file"
    assert sorted(p.name for p in (old_data / "gc-pending").iterdir()) == ["rain"], "a marker of a deleted store goes"

    assert client.get("/zarr/rain/zarr.json").status_code == 200
    assert client.get("/icechunk/rain/repo").status_code == 200
    assert client.get("/features/districts/data.parquet").status_code == 200
    assert client.get("/tables/rain_districts/rows").json()["items"][0]["value"] == 1.0
    assert client.get("/tables/rain_districts/data.parquet").status_code == 200
    assert client.get("/datasets/rain/thumbnail.png").content.endswith(b"rain")
    assert client.get("/datasets/districts/thumbnail.png").content.endswith(b"districts")
    [job] = json.loads((old_data / "openeo_jobs" / "jobs.json").read_text())
    assert job["publication"]["store_path"] == job["publication"]["record"]["path"] == str(layout.raster_store("rain"))
    assert job["usage"]["output_path"] == "managed://rain"

    records = (old_data / "artifacts" / "records.json").read_text()
    assert migrate_layout.main([]) == 0, "running it again"
    assert (old_data / "artifacts" / "records.json").read_text() == records


def test_a_dataset_whose_id_breaks_the_rule_stops_the_migration(
    old_data: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    index = old_data / "artifacts" / "records.json"
    records = json.loads(index.read_text())
    index.write_text(json.dumps([*records, {**records[0], "artifact_id": "a2", "dataset_id": "Rain.v2"}]))
    before = sorted(p.relative_to(old_data).as_posix() for p in old_data.rglob("*"))

    assert migrate_layout.main(["--dry-run"]) == 1
    assert "'Rain.v2'" in capsys.readouterr().err
    assert migrate_layout.main([]) == 1
    assert "'Rain.v2'" in capsys.readouterr().err

    assert sorted(p.relative_to(old_data).as_posix() for p in old_data.rglob("*")) == before, "nothing moved"
