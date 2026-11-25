"""The object interface every record, data file and derived file is written through."""

from __future__ import annotations

import errno
import os
import stat
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from open_geo_service import storage
from open_geo_service.features import store as feature_store
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.main import app
from open_geo_service.shared import layout
from open_geo_service.tables import store as table_store

from .test_catalog_files import _box, write_one_of_each


def test_a_put_is_whole_flushed_and_create_refuses_an_existing_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    objects = storage.Objects(root, staging=root / "tmp")
    files: list[int] = []
    folders: list[Path] = []
    fsync, flush_folder = os.fsync, storage._flush_folder
    monkeypatch.setattr(storage.os, "fsync", lambda descriptor: files.append(descriptor) or fsync(descriptor))
    monkeypatch.setattr(storage, "_flush_folder", lambda folder: folders.append(folder) or flush_folder(folder))

    objects.put("catalog/a/collection.json", b"{}")

    assert objects.get("catalog/a/collection.json") == b"{}"
    assert list((root / "tmp").iterdir()) == [], "nothing staged is left behind"
    if os.name != "nt":
        # The file, then every folder from its own up to the root, before the put returns.
        assert folders == [root / "catalog" / "a", root / "catalog", root]
        assert len(files) == 1 + len(folders), "the file itself, and each folder"
        objects.path("catalog/a/collection.json").chmod(0o640)
        objects.put("catalog/a/collection.json", b"{ }")
        assert stat.S_IMODE(objects.path("catalog/a/collection.json").stat().st_mode) == 0o640, (
            "a replace keeps its mode"
        )
    else:
        objects.put("catalog/a/collection.json", b"{ }")
    with pytest.raises(FileExistsError):
        objects.put("catalog/a/collection.json", b"[]", create=True)

    assert objects.get("catalog/a/collection.json") == b"{ }"

    # With catalog/ on another filesystem than the staging folder, as a mount or a symlinked disk.
    replace = storage.replace_file

    def across(source: str | Path, target: Path) -> None:
        if Path(source).parent != target.parent:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        replace(source, target)

    monkeypatch.setattr(storage, "replace_file", across)
    objects.put("catalog/a/collection.json", b"{  }")
    assert objects.get("catalog/a/collection.json") == b"{  }"
    assert sorted(p.name for p in objects.path("catalog/a").iterdir()) == ["collection.json"], "nothing left beside it"
    assert [listed.key for listed in objects.scan("catalog")] == ["catalog/a/collection.json"]


def test_a_write_stopped_between_file_and_record_is_removed_at_the_writers_next_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    write_one_of_each(data, monkeypatch)
    records = {record.dataset_id: record for record in ingestion_services._load_records()}
    districts = Path(str(records["districts"].path))
    table = Path(records["rain_districts"].asset_paths[0])
    # Written a day ago; what follows is newer than them, and older than the grace period.
    for path in (data / "catalog").rglob("*.p*"):
        os.utime(path, (time.time() - 86400, time.time() - 86400))

    # The first half of each write, the file, with no record after it: what a process stopped
    # between the two leaves.
    stopped_vector, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="districts",
        features={"type": "FeatureCollection", "features": [_box("D2")]},
        id_property="orgUnitCode",
    )
    stopped_table = table_store._write_file(
        layout.dataset_dir(layout.Kind.TABLE, "rain_districts"), pq.read_table(table), datetime.now(UTC)
    )
    # A first write stopped the same way, in a folder no record names at all.
    unrecorded, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="regions",
        features={"type": "FeatureCollection", "features": [_box("R1")]},
        id_property="orgUnitCode",
    )

    # Older than the grace period, as after a restart; a reader may still hold a younger one.
    for path in (stopped_vector, stopped_table, unrecorded):
        os.utime(path, (time.time() - 3600, time.time() - 3600))
    fresh, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="towns", features={"type": "FeatureCollection", "features": [_box("T1")]}, id_property="orgUnitCode"
    )

    with TestClient(app):  # the writer starts
        pass

    assert not stopped_vector.exists() and not stopped_table.exists() and not unrecorded.exists()
    assert fresh.exists(), "a file younger than the grace period waits for the next start"
    assert districts.exists() and table.exists(), "the files the records name stay"
    assert TestClient(app).get("/features/districts/data.parquet").status_code == 200
